export interface Topic {
  name: string;
  internal_name: string;
  processed_total: number;
  failed_total: number;
  retry_total: number;
  avg_duration: number | null;
  success_rate: string | null;
  paused: boolean;
  max_retries: number;
  retry_delay: number;
  process_timeout: number;
  queue_pending: number;
  queue_delayed: number;
  queue_processing: number;
  history_limit: number;
  schema?: string;
  schema_type?: string;
}

export interface Stats {
  stats: {
    active_jobs: number;
    idle_slots: number;
    queue: {
      pending: number;
      delayed: number;
      processing: number;
    };
    topics: Record<string, Topic>;
  };
  system: {
    goroutines: number;
    memory: string;
    go_version: string;
    num_cpu: number;
  };
  timestamp: number;
  uptime: number;
}

export type WSStatus = "connected" | "disconnected" | "connecting";

export interface ConfirmModalState {
  isOpen: boolean;
  title: string;
  description?: string;
  message: string | React.ReactNode;
  onConfirm: () => void;
  variant: "danger" | "warning";
}
