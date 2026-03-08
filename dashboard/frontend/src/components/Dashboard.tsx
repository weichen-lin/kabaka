import { motion } from "framer-motion";
import {
  Activity,
  Cpu,
  Globe,
  HardDrive,
  Server,
  ShieldAlert,
} from "lucide-react";
import { useStats } from "../api/queries";
import { StatCard } from "./StatCard";

export const Dashboard = () => {
  const { data: statsData, isLoading } = useStats();

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-kb-neon animate-pulse font-black italic tracking-widest uppercase">
          Syncing Control Center...
        </div>
      </div>
    );
  }

  const stats = statsData?.stats;
  const system = statsData?.system;
  const uptimeSeconds = statsData?.uptime || 0;

  const formatUptime = (seconds: number) => {
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    return [hrs, mins, secs]
      .map((v) => (v < 10 ? `0${v}` : v))
      .filter((v, i) => v !== "00" || i > 0)
      .join(":");
  };

  return (
    <div className="flex-1 overflow-y-auto p-8 space-y-8 relative z-10">
      <header>
        <h2 className="text-3xl font-black italic tracking-tighter text-kb-text uppercase leading-none">
          System Overview
        </h2>
        <p className="text-[10px] text-kb-subtext tracking-[0.3em] font-black mt-1.5 uppercase opacity-80">
          Central Intelligence & Resource Distribution
        </p>
      </header>

      {/* Aggregate Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Active Jobs"
          val={stats?.active_jobs ?? 0}
          unit="qty"
          icon={Activity}
          color="text-kb-neon"
          trend="LIVE"
        />
        <StatCard
          label="Idle Slots"
          val={stats?.idle_slots ?? 0}
          unit="qty"
          icon={Cpu}
          color="text-kb-info"
        />
        <StatCard
          label="Queue Pending"
          val={stats?.queue?.pending ?? 0}
          unit="msg"
          icon={Server}
          color="text-kb-warning"
        />
        <StatCard
          label="Queue Delayed"
          val={stats?.queue?.delayed ?? 0}
          unit="msg"
          icon={ShieldAlert}
          color="text-red-500"
        />
      </div>

      <div className="space-y-6">
        {/* Worker Fleet Monitor - Full Width */}
        <section className="bg-kb-card border border-kb-border p-8 relative overflow-hidden">
          <div className="flex justify-between items-center mb-8">
            <h3 className="text-xs font-black uppercase tracking-[0.2em] flex items-center gap-3">
              <Server size={16} className="text-kb-neon" /> Worker Fleet
              Real-time Allocation
            </h3>
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-kb-neon shadow-[0_0_8px_var(--kb-neon)]" />
                <span className="text-[9px] font-black text-kb-text uppercase">
                  Active: {stats?.active_jobs || 0}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-kb-border" />
                <span className="text-[9px] font-black text-kb-subtext uppercase">
                  Idle: {stats?.idle_slots || 0}
                </span>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-10 sm:grid-cols-25 md:grid-cols-50 gap-3">
            {Array.from({
              length: (stats?.active_jobs || 0) + (stats?.idle_slots || 0),
            }).map((_, i) => (
              <motion.div
                // biome-ignore lint/suspicious/noArrayIndexKey: visualization slots have no unique stable ID
                key={`slot-${i}`}
                initial={false}
                animate={{
                  backgroundColor:
                    i < (stats?.active_jobs || 0)
                      ? "var(--kb-neon)"
                      : "rgba(255,255,255,0.05)",
                  boxShadow:
                    i < (stats?.active_jobs || 0)
                      ? "0 0 12px var(--kb-neon)"
                      : "none",
                  scale: i < (stats?.active_jobs || 0) ? [1, 1.2, 1] : 1,
                }}
                transition={{ type: "spring", stiffness: 300, damping: 20 }}
                className="h-4 w-full min-w-[4px] rounded-sm border"
              />
            ))}
          </div>
        </section>

        {/* System Environment - Full Width */}
        <section className="bg-kb-card border border-kb-border p-8 relative overflow-hidden">
          <div className="absolute top-0 right-0 p-8 opacity-5">
            <Globe size={160} />
          </div>

          <h3 className="text-xs font-black uppercase tracking-[0.2em] mb-8 flex items-center gap-3">
            <HardDrive size={16} className="text-kb-info" /> Core Runtime
            Diagnostics
          </h3>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-12 relative z-10">
            <div className="space-y-2 border-l-2 border-kb-border pl-6">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.2em]">
                Go Runtime
              </div>
              <div className="text-xl font-black text-kb-text italic tracking-tighter">
                {system?.go_version?.split(" ")[0] || "N/A"}
              </div>
            </div>
            <div className="space-y-2 border-l-2 border-kb-border pl-6">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.2em]">
                Active Goroutines
              </div>
              <div className="text-xl font-black text-kb-neon italic tracking-tighter">
                {system?.goroutines || 0}
              </div>
            </div>
            <div className="space-y-2 border-l-2 border-kb-border pl-6">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.2em]">
                Heap Allocation
              </div>
              <div className="text-xl font-black text-kb-info italic tracking-tighter">
                {system?.memory || "0 MB"}
              </div>
            </div>
            <div className="space-y-2 border-l-2 border-kb-border pl-6">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.2em]">
                System Uptime
              </div>
              <div className="text-xl font-black text-kb-warning italic tracking-tighter tabular-nums">
                {formatUptime(uptimeSeconds)}
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  );
};
