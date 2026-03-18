import { motion } from "framer-motion";
import { Clock, Heart, Layers, Monitor, Users } from "lucide-react";
import type { InstanceInfo } from "../types";

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const seconds = Math.floor(diff / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}

function instanceUptime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const seconds = Math.floor(diff / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ${minutes % 60}m`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

function isHealthy(lastHeartbeat: string): boolean {
  return Date.now() - new Date(lastHeartbeat).getTime() < 30_000;
}

interface Props {
  instances: InstanceInfo[];
}

export const InstancesOverview = ({ instances }: Props) => {
  const totalWorkers = instances.reduce((sum, i) => sum + i.workers, 0);
  const healthyCount = instances.filter((i) =>
    isHealthy(i.last_heartbeat),
  ).length;

  return (
    <section className="bg-kb-card border border-kb-border p-5 relative overflow-hidden h-full flex flex-col">
      <div className="absolute top-0 right-0 p-8 opacity-5">
        <Layers
          size={160}
          className="text-kb-neon animate-[spin_90s_linear_infinite]"
        />
      </div>

      <div className="flex items-center justify-between mb-4 shrink-0">
        <h3 className="text-xs font-black uppercase tracking-[0.3em] flex items-center gap-3">
          <div className="w-8 h-8 bg-kb-bg border border-kb-neon/20 flex items-center justify-center">
            <Layers size={16} className="text-kb-neon" />
          </div>
          <div className="flex flex-col">
            Instance Fleet
            <span className="text-[8px] text-kb-subtext/50 tracking-widest mt-0.5 italic">
              CLUSTER NODES
            </span>
          </div>
        </h3>

        <div className="flex items-center gap-4 text-[9px] font-black uppercase tracking-widest">
          <div className="flex items-center gap-1.5 text-kb-neon">
            <div className="w-1.5 h-1.5 bg-kb-neon shadow-[0_0_8px_var(--kb-neon)]" />
            {healthyCount} ONLINE
          </div>
          <div className="text-kb-subtext/50">|</div>
          <div className="flex items-center gap-1.5 text-kb-subtext">
            <Users size={10} />
            {totalWorkers} WORKERS
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto space-y-3 custom-scrollbar relative z-10">
        {instances.map((instance, idx) => {
          const healthy = isHealthy(instance.last_heartbeat);
          return (
            <motion.div
              key={instance.id}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: idx * 0.05 }}
              className={`group relative bg-kb-bg border p-4 transition-all hover:border-kb-neon/40 ${
                healthy ? "border-kb-border" : "border-kb-warning/30"
              }`}
            >
              {healthy && (
                <div className="absolute top-0 left-0 w-full h-[2px] bg-kb-neon shadow-[0_0_10px_var(--kb-neon)]" />
              )}

              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <div
                    className={`w-6 h-6 border flex items-center justify-center ${
                      healthy
                        ? "border-kb-neon/30 text-kb-neon"
                        : "border-kb-warning/30 text-kb-warning"
                    }`}
                  >
                    <Monitor size={12} />
                  </div>
                  <div>
                    <div className="text-xs font-black text-kb-text tracking-tight">
                      {instance.hostname}
                    </div>
                    <div className="text-[7px] text-kb-subtext/40 font-mono">
                      {instance.id.slice(0, 8)}
                    </div>
                  </div>
                </div>

                <div
                  className={`flex items-center gap-1 text-[8px] font-black uppercase tracking-widest px-1.5 py-0.5 border ${
                    healthy
                      ? "text-kb-neon border-kb-neon/20 bg-kb-neon/5"
                      : "text-kb-warning border-kb-warning/20 bg-kb-warning/5 animate-pulse"
                  }`}
                >
                  <Heart
                    size={7}
                    className={
                      healthy ? "animate-[pulse_2s_ease-in-out_infinite]" : ""
                    }
                  />
                  {healthy ? "ALIVE" : "STALE"}
                </div>
              </div>

              <div className="grid grid-cols-3 gap-3">
                <div className="space-y-0.5">
                  <div className="text-[7px] font-black text-kb-subtext/60 uppercase tracking-[0.2em]">
                    Workers
                  </div>
                  <div className="text-base font-black text-kb-text italic tracking-tighter">
                    {instance.workers}
                  </div>
                </div>
                <div className="space-y-0.5">
                  <div className="text-[7px] font-black text-kb-subtext/60 uppercase tracking-[0.2em]">
                    Uptime
                  </div>
                  <div className="text-base font-black text-kb-info italic tracking-tighter">
                    {instanceUptime(instance.started_at)}
                  </div>
                </div>
                <div className="space-y-0.5">
                  <div className="text-[7px] font-black text-kb-subtext/60 uppercase tracking-[0.2em] flex items-center gap-0.5">
                    <Clock size={7} />
                    Heartbeat
                  </div>
                  <div
                    className={`text-base font-black italic tracking-tighter ${
                      healthy ? "text-kb-neon" : "text-kb-warning"
                    }`}
                  >
                    {timeAgo(instance.last_heartbeat)}
                  </div>
                </div>
              </div>
            </motion.div>
          );
        })}

        {instances.length === 0 && (
          <div className="text-center py-10 text-kb-subtext/40">
            <Layers size={36} className="mx-auto mb-3 opacity-30" />
            <div className="text-xs font-black uppercase tracking-[0.3em]">
              No Instances Registered
            </div>
          </div>
        )}
      </div>
    </section>
  );
};
