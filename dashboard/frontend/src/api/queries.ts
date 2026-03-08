import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";

const API_BASE = "/api/v1";

export interface Topic {
  name: string;
  processedTotal: number;
  failedTotal: number;
  retryTotal: number;
  avgDuration: number;
  queueStats?: {
    pending: number;
    delayed: number;
    processing: number;
  };
  successRate: string;
}

export interface Stats {
  stats: {
    ActiveJobs: number;
    IdleSlots: number;
    Queue: {
      Pending: number;
      Delayed: number;
      Processing: number;
    };
    Topics: Record<string, Topic>;
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

// WebSocket Hook: Listening for server pushes and manually updating the Query Cache
export const useWebSocket = () => {
  const queryClient = useQueryClient();

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}${API_BASE}/ws`;

    let socket: WebSocket;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    const connect = () => {
      socket = new WebSocket(wsUrl);

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === "stats") {
            // Update the TanStack Query cache with the full data object from Go
            queryClient.setQueryData(["stats"], (old: Stats | undefined) => {
              if (!old) return message.data;
              return {
                ...message.data,
                timestamp: message.timestamp,
              };
            });
          }
        } catch (err) {
          console.error("WS parse error:", err);
        }
      };

      socket.onclose = () => {
        reconnectTimer = setTimeout(connect, 2000);
      };
    };

    connect();

    return () => {
      if (socket) socket.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [queryClient]);
};
