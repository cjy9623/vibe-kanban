use std::collections::HashMap;

use anyhow;
use axum::{
    Extension, Router,
    extract::{Path, Query, State, ws::Message},
    middleware::from_fn_with_state,
    response::{IntoResponse, Json as ResponseJson},
    routing::{get, post},
};
use db::models::{
    execution_process::{ExecutionProcess, ExecutionProcessStatus},
    execution_process_repo_state::ExecutionProcessRepoState,
};
use deployment::Deployment;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use services::services::{container::ContainerService, execution_process};
use ts_rs::TS;
use utils::{log_msg::LogMsg, response::ApiResponse};
use uuid::Uuid;

use crate::{
    DeploymentImpl,
    error::ApiError,
    middleware::{
        load_execution_process_middleware,
        signed_ws::{MaybeSignedWebSocket, SignedWsUpgrade},
    },
};

#[derive(Debug, Deserialize)]
struct SessionExecutionProcessQuery {
    pub session_id: Uuid,
    /// If true, include soft-deleted (dropped) processes in results/stream
    #[serde(default)]
    pub show_soft_deleted: Option<bool>,
}

async fn get_execution_process_by_id(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(_deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<ExecutionProcess>>, ApiError> {
    Ok(ResponseJson(ApiResponse::success(execution_process)))
}

async fn stream_raw_logs_ws(
    ws: SignedWsUpgrade,
    State(deployment): State<DeploymentImpl>,
    Path(exec_id): Path<Uuid>,
) -> impl IntoResponse {
    // Always accept the WebSocket upgrade — handle "not found" inside the
    // connection by sending `finished` and closing cleanly, instead of
    // rejecting with HTTP 404 which the browser surfaces as an opaque
    // connection failure.
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_raw_logs_ws(socket, deployment, exec_id).await {
            tracing::warn!("raw logs WS closed: {}", e);
        }
    })
}

async fn handle_raw_logs_ws(
    mut socket: MaybeSignedWebSocket,
    deployment: DeploymentImpl,
    exec_id: Uuid,
) -> anyhow::Result<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use executors::logs::utils::patch::ConversationPatch;
    use utils::log_msg::LogMsg;

    // Get the raw stream — if not found, send finished and close cleanly
    let raw_stream = match deployment.container().stream_raw_logs(&exec_id).await {
        Some(stream) => stream,
        None => {
            // No logs available: send finished so the client gets a clean
            // close instead of retrying endlessly.
            let _ = socket
                .send(LogMsg::Finished.to_ws_message_unchecked())
                .await;
            let _ = socket.close().await;
            return Ok(());
        }
    };

    let counter = Arc::new(AtomicUsize::new(0));
    let mut stream = raw_stream.map_ok({
        let counter = counter.clone();
        move |m| match m {
            LogMsg::Stdout(content) => {
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let patch = ConversationPatch::add_stdout(index, content);
                LogMsg::JsonPatch(patch).to_ws_message_unchecked()
            }
            LogMsg::Stderr(content) => {
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let patch = ConversationPatch::add_stderr(index, content);
                LogMsg::JsonPatch(patch).to_ws_message_unchecked()
            }
            LogMsg::Finished => LogMsg::Finished.to_ws_message_unchecked(),
            _ => unreachable!("Raw stream should only have Stdout/Stderr/Finished"),
        }
    });

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("stream error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Ok(Some(Message::Close(_))) => break,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
    // Send a proper close frame so the client sees code 1000 (normal closure)
    // instead of an abnormal TCP drop that triggers reconnection attempts.
    let _ = socket.close().await;
    Ok(())
}

async fn stream_normalized_logs_ws(
    ws: SignedWsUpgrade,
    State(deployment): State<DeploymentImpl>,
    Path(exec_id): Path<Uuid>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        let stream = deployment
            .container()
            .stream_normalized_logs(&exec_id)
            .await;

        match stream {
            Some(stream) => {
                let stream = stream.err_into::<anyhow::Error>().into_stream();
                if let Err(e) = handle_normalized_logs_ws(socket, stream).await {
                    tracing::warn!("normalized logs WS closed: {}", e);
                }
            }
            None => {
                // No logs available: send finished and close cleanly
                let mut socket = socket;
                let _ = socket
                    .send(utils::log_msg::LogMsg::Finished.to_ws_message_unchecked())
                    .await;
                let _ = socket.close().await;
            }
        }
    })
}

async fn handle_normalized_logs_ws(
    mut socket: MaybeSignedWebSocket,
    stream: impl futures_util::Stream<Item = anyhow::Result<LogMsg>> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let mut stream = stream.map_ok(|msg| msg.to_ws_message_unchecked());
    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("stream error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Ok(Some(Message::Close(_))) => break,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
    let _ = socket.close().await;
    Ok(())
}

async fn stop_execution_process(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<()>>, ApiError> {
    deployment
        .container()
        .stop_execution(&execution_process, ExecutionProcessStatus::Killed)
        .await?;

    Ok(ResponseJson(ApiResponse::success(())))
}

async fn stream_execution_processes_by_session_ws(
    ws: SignedWsUpgrade,
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<SessionExecutionProcessQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_execution_processes_by_session_ws(
            socket,
            deployment,
            query.session_id,
            query.show_soft_deleted.unwrap_or(false),
        )
        .await
        {
            tracing::warn!("execution processes by session WS closed: {}", e);
        }
    })
}

async fn handle_execution_processes_by_session_ws(
    mut socket: MaybeSignedWebSocket,
    deployment: DeploymentImpl,
    session_id: uuid::Uuid,
    show_soft_deleted: bool,
) -> anyhow::Result<()> {
    // Get the raw stream and convert LogMsg to WebSocket messages
    let mut stream = deployment
        .events()
        .stream_execution_processes_for_session_raw(session_id, show_soft_deleted)
        .await?
        .map_ok(|msg| msg.to_ws_message_unchecked());

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("stream error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Ok(Some(Message::Close(_))) => break,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
    Ok(())
}

async fn get_execution_process_repo_states(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<Vec<ExecutionProcessRepoState>>>, ApiError> {
    let pool = &deployment.db().pool;
    let repo_states =
        ExecutionProcessRepoState::find_by_execution_process_id(pool, execution_process.id).await?;
    Ok(ResponseJson(ApiResponse::success(repo_states)))
}

async fn get_execution_process_turn(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<Option<serde_json::Value>>>, ApiError> {
    let pool = &deployment.db().pool;
    let raw = execution_process::read_execution_logs_for_execution(pool, execution_process.id)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to read logs for {}: {}", execution_process.id, e);
            None
        })
        .unwrap_or_default();

    // Parse and filter stream_events (same logic as logs endpoint)
    let stream_events: Vec<serde_json::Value> = raw
        .lines()
        .filter_map(|line| {
            let outer: serde_json::Value = serde_json::from_str(line).ok()?;
            let inner_str = outer.get("Stdout")?.as_str()?;
            let inner: serde_json::Value = serde_json::from_str(inner_str).ok()?;
            let t = inner.get("type")?.as_str()?;
            if t == "system" && inner.get("subtype")?.as_str()? == "thinking_tokens" {
                return None;
            }
            if t == "stream_event" {
                let et = inner.get("event")?.get("type")?.as_str()?;
                if et == "content_block_delta"
                    && inner.get("event")?.get("delta")?.get("type")?.as_str()? == "thinking_delta"
                {
                    return None;
                }
            }
            Some(inner)
        })
        .filter(|v| v.get("type").and_then(|x| x.as_str()) == Some("stream_event"))
        .collect();

    let messages = aggregate_stream_events(&stream_events);
    let last = messages.into_iter().last();

    Ok(ResponseJson(ApiResponse::success(last)))
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    page: Option<u64>,
    limit: Option<u64>,
}

#[derive(Debug, Serialize, TS)]
struct LogsResponse {
    messages: Vec<serde_json::Value>,
    total: u64,
    page: u64,
    limit: u64,
}

/// Aggregate CC stream_event deltas into coherent structured messages.
fn aggregate_stream_events(events: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut messages: Vec<serde_json::Value> = Vec::new();
    // Current content blocks being built: (block_index -> "tool_use" | "text", accumulated_text)
    let mut blocks: HashMap<i64, (String, String)> = HashMap::new();
    let mut current_role: Option<String> = None;

    for event in events {
        let evt = match event.get("event") {
            Some(e) => e,
            None => {
                // Non-stream-event: pass through directly
                messages.push(event.clone());
                continue;
            }
        };

        match evt.get("type").and_then(|v| v.as_str()) {
            Some("message_start") => {
                // Flush previous message before starting a new one
                if !blocks.is_empty() {
                    let msg = build_message(&mut blocks, &current_role);
                    if !msg["content"].as_array().map_or(true, |a| a.is_empty()) {
                        messages.push(msg);
                    }
                }
                if let Some(msg) = evt.get("message") {
                    if let Some(role) = msg.get("role").and_then(|v| v.as_str()) {
                        current_role = Some(role.to_string());
                    }
                }
            }
            Some("content_block_start") => {
                let idx = evt.get("index").and_then(|v| v.as_i64()).unwrap_or(0);
                if let Some(cb) = evt.get("content_block") {
                    let block_type = cb.get("type").and_then(|v| v.as_str()).unwrap_or("text");
                    let text = cb.get("text").and_then(|v| v.as_str()).unwrap_or("");
                    blocks.insert(idx, (block_type.to_string(), text.to_string()));
                }
            }
            Some("content_block_delta") => {
                let idx = evt.get("index").and_then(|v| v.as_i64()).unwrap_or(0);
                if let Some(delta) = evt.get("delta") {
                    match delta.get("type").and_then(|v| v.as_str()) {
                        Some("text_delta") => {
                            if let Some(t) = delta.get("text").and_then(|v| v.as_str()) {
                                blocks
                                    .entry(idx)
                                    .and_modify(|(_, acc)| acc.push_str(t))
                                    .or_insert_with(|| ("text".to_string(), t.to_string()));
                            }
                        }
                        Some("input_json_delta") => {
                            if let Some(t) = delta.get("partial_json").and_then(|v| v.as_str()) {
                                blocks
                                    .entry(idx)
                                    .and_modify(|(_, acc)| acc.push_str(t))
                                    .or_insert_with(|| ("tool_use".to_string(), t.to_string()));
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("content_block_stop") | Some("message_stop") => {
                // Nothing to do on stop — we emit at the next message_start or at end
            }
            _ => {}
        }
    }

    // Emit accumulated blocks as structured messages
    if !blocks.is_empty() {
        let msg = build_message(&mut blocks, &current_role);
        if !msg["content"].as_array().map_or(true, |a| a.is_empty()) {
            messages.push(msg);
        }
    }

    messages
}

fn build_message(
    blocks: &mut HashMap<i64, (String, String)>,
    current_role: &Option<String>,
) -> serde_json::Value {
    let mut sorted: Vec<(i64, (String, String))> = blocks.drain().collect();
    sorted.sort_by_key(|(k, _)| *k);

    let mut content_blocks: Vec<serde_json::Value> = Vec::new();
    for (_, (block_type, text)) in &sorted {
        if text.is_empty() {
            continue;
        }
        if block_type == "text" {
            content_blocks.push(serde_json::json!({"type": "text", "text": text}));
        }
        // tool_use blocks are skipped — only text content is returned
    }

    serde_json::json!({
        "role": current_role.as_deref().unwrap_or("assistant"),
        "content": content_blocks
    })
}

async fn get_execution_process_logs(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<LogsQuery>,
) -> Result<ResponseJson<ApiResponse<LogsResponse>>, ApiError> {
    let pool = &deployment.db().pool;
    let raw = execution_process::read_execution_logs_for_execution(pool, execution_process.id)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to read logs for {}: {}", execution_process.id, e);
            None
        })
        .unwrap_or_default();

    // 1. Parse everything, filter noise (thinking_tokens, thinking_delta)
    let parsed: Vec<serde_json::Value> = raw
        .lines()
        .filter_map(|line| {
            let outer: serde_json::Value = serde_json::from_str(line).ok()?;
            let inner_str = outer.get("Stdout")?.as_str()?;
            let inner: serde_json::Value = serde_json::from_str(inner_str).ok()?;
            let t = inner.get("type")?.as_str()?;
            if t == "system" && inner.get("subtype")?.as_str()? == "thinking_tokens" {
                return None;
            }
            if t == "stream_event" {
                let et = inner.get("event")?.get("type")?.as_str()?;
                if et == "content_block_delta"
                    && inner.get("event")?.get("delta")?.get("type")?.as_str()? == "thinking_delta"
                {
                    return None;
                }
            }
            Some(inner)
        })
        .collect();

    // 2. Only show stream events (the actual content stream)
    let stream_events: Vec<serde_json::Value> = parsed
        .into_iter()
        .filter(|v| v.get("type").and_then(|x| x.as_str()) == Some("stream_event"))
        .collect();

    // 3. Aggregate deltas into structured messages
    let all_messages = aggregate_stream_events(&stream_events);

    let total = all_messages.len() as u64;
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let page = query.page.unwrap_or(1).max(1);
    let offset = ((page - 1) * limit).min(total);
    let end = (offset + limit).min(total);
    let messages: Vec<serde_json::Value> = all_messages[offset as usize..end as usize].to_vec();

    Ok(ResponseJson(ApiResponse::success(LogsResponse {
        messages,
        total,
        page,
        limit,
    })))
}

pub(super) fn router(deployment: &DeploymentImpl) -> Router<DeploymentImpl> {
    let workspace_id_router = Router::new()
        .route("/", get(get_execution_process_by_id))
        .route("/turn", get(get_execution_process_turn))
        .route("/logs", get(get_execution_process_logs))
        .route("/stop", post(stop_execution_process))
        .route("/repo-states", get(get_execution_process_repo_states))
        .route("/raw-logs/ws", get(stream_raw_logs_ws))
        .route("/normalized-logs/ws", get(stream_normalized_logs_ws))
        .layer(from_fn_with_state(
            deployment.clone(),
            load_execution_process_middleware,
        ));

    let workspaces_router = Router::new()
        .route(
            "/stream/session/ws",
            get(stream_execution_processes_by_session_ws),
        )
        .nest("/{id}", workspace_id_router);

    Router::new().nest("/execution-processes", workspaces_router)
}
