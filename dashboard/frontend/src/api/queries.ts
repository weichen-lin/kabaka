import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { useStore } from "../store/useStore";
import type { Stats, Topic } from "../types";
export type { Topic };

const API_BASE = "/api/v1";

export const useStats = () => {
  return useQuery<Stats>({
    queryKey: ["stats"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/stats`);
      if (!res.ok) throw new Error("Network response was not ok");
      const text = await res.text();
      return text ? JSON.parse(text) : {};
    },
  });
};

export const useTopics = () => {
  return useQuery<{ topics: Topic[] }>({
    queryKey: ["topics"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/topics`);
      if (!res.ok) throw new Error("Network response was not ok");
      const text = await res.text();
      return text ? JSON.parse(text) : {};
    },
  });
};

export const useTopicDetail = (name: string | null) => {
  return useQuery<Topic>({
    queryKey: ["topics", name],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/topics/${name}`);
      if (!res.ok) throw new Error("Topic not found");
      const text = await res.text();
      return text ? JSON.parse(text) : {};
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
      const text = await res.text();
      return text ? JSON.parse(text) : {};
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
      const text = await res.text();
      return text ? JSON.parse(text) : {};
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
      const text = await res.text();
      return text ? JSON.parse(text) : {};
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
      const text = await res.text();
      return text ? JSON.parse(text) : {};
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
  const reconnectAttempts = useRef(0);

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
        reconnectAttempts.current = 0;
      };

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === "stats") {
            const statsData = message.data as Stats;

            queryClient.setQueryData(["stats"], (old: Stats | undefined) => {
              if (!old) return statsData;
              return {
                ...statsData,
                timestamp: message.timestamp,
              };
            });

            if (statsData.stats?.topics) {
              const topicsArray = Object.entries(statsData.stats.topics)
                .map(([name, topic]) => {
                  const topicData: Topic = {
                    ...topic,
                    name: name,
                  };
                  // Sync specific topic detail cache
                  queryClient.setQueryData(["topics", name], topicData);
                  return topicData;
                })
                .sort((a, b) => a.name.localeCompare(b.name));
              queryClient.setQueryData(["topics"], { topics: topicsArray });
            }
          }
        } catch (err) {
          console.error("WS parse error:", err);
        }
      };

      socket.onclose = () => {
        setWSStatus("disconnected");

        // Exponential backoff: 1s, 2s, 4s, 8s, up to 30s
        const backoff = Math.min(1000 * 2 ** reconnectAttempts.current, 30000);
        reconnectAttempts.current += 1;

        reconnectTimer = setTimeout(connect, backoff);
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
