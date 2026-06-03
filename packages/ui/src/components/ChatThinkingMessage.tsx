import type { ReactNode } from 'react';
import { ChatDotsIcon } from '@phosphor-icons/react';
import { useTranslation } from 'react-i18next';
import { cn } from '../lib/cn';

export interface ChatThinkingMessageRenderProps {
  content: string;
  workspaceId?: string;
  className?: string;
}

interface ChatThinkingMessageProps {
  content: string;
  className?: string;
  workspaceId?: string;
  isActive?: boolean;
  renderMarkdown: (props: ChatThinkingMessageRenderProps) => ReactNode;
}

export function ChatThinkingMessage({
  content,
  className,
  workspaceId,
  isActive = false,
  renderMarkdown,
}: ChatThinkingMessageProps) {
  const { t } = useTranslation('common');
  const displayContent =
    content.trim().length > 0
      ? content
      : isActive
        ? t('conversation.thinking')
        : '';

  return (
    <div
      className={cn('flex items-start gap-base text-sm text-low', className)}
    >
      <ChatDotsIcon className="shrink-0 size-icon-base pt-0.5" />
      {renderMarkdown({
        content: displayContent,
        workspaceId: workspaceId,
        className: 'text-sm',
      })}
    </div>
  );
}
