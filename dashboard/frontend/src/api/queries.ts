import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";

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

  return {
    pause: pauseMutation.mutate,
    resume: resumeMutation.mutate,
    purge: purgeMutation.mutate,
    isPausing: pauseMutation.isPending,
    isResuming: resumeMutation.isPending,
    isPurging: purgeMutation.isPending,
  };
};

import { useStore } from "../store/useStore";

// ... (Topic, Stats interfaces and useStats, useTopics keep unchanged)

// WebSocket Hook: Listening for server pushes and manually updating the Query Cache
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

            // 1. Update overall stats
            queryClient.setQueryData(["stats"], (old: Stats | undefined) => {
              if (!old) return statsData;
              return {
                ...statsData,
                timestamp: message.timestamp,
              };
            });

            // 2. Sync Topic strategy list immediately
            if (statsData.stats?.topics) {
              const topicsArray = Object.entries(statsData.stats.topics).map(
                ([name, topic]) => ({
                  ...(topic as Topic),
                  name: name,
                }),
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
