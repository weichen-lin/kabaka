import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { useStore } from "../store/useStore";

const API_BASE = "/api/v1";

export interface Topic {
  name: string;
  internal_name: string;
  processed_total: number;
  failed_total: number;
  retry_total: number;
  avg_duration: number;
  success_rate: string;
  paused: boolean;
  max_retries: number;
  retry_delay: number;
  process_timeout: number;
  queue_pending: number;
  queue_delayed: number;
  queue_processing: number;
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

export const useStats = () => {
  return useQuery<Stats>({
    queryKey: ["stats"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/stats`);
      if (!res.ok) throw new Error("Network response was not ok");
      return res.json();
    },
  });
};

export const useTopics = () => {
  return useQuery<{ topics: Topic[] }>({
    queryKey: ["topics"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/topics`);
      if (!res.ok) throw new Error("Network response was not ok");
      return res.json();
    },
  });
};

export const useTopicDetail = (name: string | null) => {
  return useQuery<Topic>({
    queryKey: ["topics", name],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/topics/${name}`);
      if (!res.ok) throw new Error("Topic not found");
      return res.json();
    },
    enabled: !!name,
  });
};

export const useTopicActions = () => {
  const queryClient = useQueryClient();

  const pauseMutation = useMutation({
    mutationFn: async (name: string) => {
      const res = await fetch(`${API_BASE}/topics/${name}/pause`, {
        method: "POST",
      });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["topics"] });
      queryClient.invalidateQueries({ queryKey: ["stats"] });
    },
  });

  const resumeMutation = useMutation({
    mutationFn: async (name: string) => {
      const res = await fetch(`${API_BASE}/topics/${name}/resume`, {
        method: "POST",
      });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["topics"] });
      queryClient.invalidateQueries({ queryKey: ["stats"] });
    },
  });

  const purgeMutation = useMutation({
    mutationFn: async (name: string) => {
      const res = await fetch(`${API_BASE}/topics/${name}/purge`, {
        method: "POST",
      });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["topics"] });
      queryClient.invalidateQueries({ queryKey: ["stats"] });
    },
  });

  const publishMutation = useMutation({
    mutationFn: async ({ name, data }: { name: string; data: unknown }) => {
      const res = await fetch(`${API_BASE}/topics/${name}/publish`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(errorText || "Failed to publish message");
      }
      return res.json();
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ["topics", variables.name] });
      queryClient.invalidateQueries({ queryKey: ["stats"] });
    },
  });

  return {
    pause: pauseMutation.mutate,
    resume: resumeMutation.mutate,
    purge: purgeMutation.mutate,
    publish: publishMutation.mutateAsync,
    isPausing: pauseMutation.isPending,
    isResuming: resumeMutation.isPending,
    isPurging: purgeMutation.isPending,
    isPublishing: publishMutation.isPending,
  };
};

export const useWebSocket = () => {
  const queryClient = useQueryClient();
  const { setWSStatus } = useStore();

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}${API_BASE}/ws`;

    let socket: WebSocket;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    const connect = () => {
      setWSStatus("connecting");
      socket = new WebSocket(wsUrl);

      socket.onopen = () => {
        setWSStatus("connected");
      };

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === "stats") {
            const statsData = message.data;

            queryClient.setQueryData(["stats"], (old: Stats | undefined) => {
              if (!old) return statsData;
              return {
                ...statsData,
                timestamp: message.timestamp,
              };
            });

            if (statsData.stats?.topics) {
              const topicsArray = Object.entries(statsData.stats.topics).map(
                ([name, topic]) => {
                  const topicData = {
                    ...(topic as Topic),
                    name: name,
                  };
                  // 同步更新「詳細資料」緩存，讓個別 Topic 頁面也能即時跳動
                  queryClient.setQueryData(["topics", name], topicData);
                  return topicData;
                },
              );
              queryClient.setQueryData(["topics"], { topics: topicsArray });
            }
          }
        } catch (err) {
          console.error("WS parse error:", err);
        }
      };

      socket.onclose = () => {
        setWSStatus("disconnected");
        reconnectTimer = setTimeout(connect, 3000);
      };

      socket.onerror = () => {
        socket.close();
      };
    };

    connect();

    return () => {
      if (socket) socket.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [queryClient, setWSStatus]);
};
