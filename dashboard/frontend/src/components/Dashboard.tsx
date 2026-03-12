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
import { formatUptime } from "../utils/format";
import { StatCard } from "./StatCard";

export const Dashboard = () => {
  const { data: statsData, isLoading } = useStats();

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="relative">
          <div className="w-16 h-16 border-4 border-kb-neon border-t-transparent animate-spin" />
          <div className="absolute inset-0 flex items-center justify-center text-[8px] font-black text-kb-neon animate-pulse">
            Loading
          </div>
        </div>
      </div>
    );
  }

  const stats = statsData?.stats;
  const system = statsData?.system;
  const uptimeSeconds = statsData?.uptime || 0;

  return (
    <div className="flex-1 h-full overflow-y-auto p-8 space-y-10 relative z-10 custom-scrollbar">
      <header className="flex justify-between items-end border-b border-kb-border pb-8">
        <h2 className="text-4xl font-black italic tracking-tighter text-kb-text uppercase leading-none">
          System Overview
        </h2>
      </header>

      {/* Aggregate Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
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

      <div className="space-y-8">
        {/* Worker Fleet Monitor - Full Width */}
        <section className="bg-kb-card border border-kb-border p-10 relative overflow-hidden group">
          <div className="absolute top-0 left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-kb-neon/20 to-transparent" />

          <div className="flex justify-between items-center mb-10">
            <div className="flex items-center gap-4">
              <div className="w-10 h-10 bg-kb-bg border border-kb-neon/20 flex items-center justify-center group-hover:border-kb-neon/50 transition-colors">
                <Server size={20} className="text-kb-neon" />
              </div>
              <h3 className="text-xs font-black uppercase tracking-[0.3em] flex flex-col">
                Worker Fleet Monitor
                <span className="text-[8px] text-kb-subtext/50 tracking-widest mt-1 italic">
                  REAL TIME NODE ALLOCATION
                </span>
              </h3>
            </div>

            <div className="flex items-center gap-8 bg-kb-bg/50 px-6 py-4 border border-kb-border">
              <div className="flex items-center gap-3">
                <div className="w-2.5 h-2.5 rounded-sm bg-kb-neon shadow-[0_0_10px_rgba(0,255,159,0.4)]" />
                <span className="text-[10px] font-black text-kb-text uppercase tracking-widest leading-none">
                  ACTIVE SLOTS: {stats?.active_jobs || 0}
                </span>
              </div>
              <div className="w-[1px] h-4 bg-kb-border" />
              <div className="flex items-center gap-3">
                <div className="w-2.5 h-2.5 rounded-sm bg-kb-border" />
                <span className="text-[10px] font-black text-kb-subtext uppercase tracking-widest leading-none">
                  IDLE SLOTS: {stats?.idle_slots || 0}
                </span>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-10 sm:grid-cols-20 md:grid-cols-40 lg:grid-cols-50 gap-2 relative z-10 p-4 bg-kb-bg/20 border border-kb-border/30">
            {Array.from({
              length: (stats?.active_jobs || 0) + (stats?.idle_slots || 0),
            }).map((_, i) => (
              <motion.div
                // biome-ignore lint/suspicious/noArrayIndexKey: visualization slots have no unique stable ID
                key={`slot-${i}`}
                initial={{ opacity: 0, scale: 0.5 }}
                animate={{
                  opacity: 1,
                  scale: 1,
                  backgroundColor:
                    i < (stats?.active_jobs || 0)
                      ? "var(--kb-neon)"
                      : "rgba(255,255,255,0.03)",
                  boxShadow:
                    i < (stats?.active_jobs || 0)
                      ? "0 0 12px var(--kb-neon)"
                      : "none",
                }}
                transition={{
                  delay: (i % 50) * 0.005,
                  type: "spring",
                  stiffness: 400,
                  damping: 30,
                }}
                className="h-5 w-full min-w-[5px] border border-kb-border/50 group/slot relative overflow-hidden"
              >
                {i < (stats?.active_jobs || 0) && (
                  <motion.div
                    animate={{ x: ["-100%", "200%"] }}
                    transition={{
                      duration: 1.5,
                      repeat: Infinity,
                      ease: "linear",
                    }}
                    className="absolute inset-0 bg-white/20 skew-x-12"
                  />
                )}
              </motion.div>
            ))}
          </div>
        </section>

        {/* System Environment - Full Width */}
        <section className="bg-kb-card border border-kb-border p-10 relative overflow-hidden">
          <div className="absolute top-0 right-0 p-12 opacity-5">
            <Globe
              size={240}
              className="text-kb-info animate-[spin_60s_linear_infinite]"
            />
          </div>

          <h3 className="text-xs font-black uppercase tracking-[0.3em] mb-10 flex items-center gap-4">
            <div className="w-10 h-10 bg-kb-bg border border-kb-info/20 flex items-center justify-center">
              <HardDrive size={20} className="text-kb-info" />
            </div>
            <div className="flex flex-col">
              System Runtime Diagnostics
              <span className="text-[8px] text-kb-subtext/50 tracking-widest mt-1 italic">
                CORE ENVIRONMENT
              </span>
            </div>
          </h3>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-12 relative z-10">
            <div className="space-y-3 border-l-2 border-kb-border/50 pl-8 group hover:border-kb-neon/50 transition-colors">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-text transition-colors">
                Go Runtime
              </div>
              <div className="text-2xl font-black text-kb-text italic tracking-tighter bg-kb-bg/40 px-3 py-1 border border-kb-border/30">
                {system?.go_version?.split(" ")[0] || "N/A"}
              </div>
            </div>
            <div className="space-y-3 border-l-2 border-kb-border/50 pl-8 group hover:border-kb-neon/50 transition-colors">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-neon transition-colors">
                Active Goroutines
              </div>
              <div className="text-2xl font-black text-kb-neon italic tracking-tighter bg-kb-bg/40 px-3 py-1 border border-kb-border/30 shadow-[0_0_15px_rgba(0,255,159,0.1)]">
                {system?.goroutines || 0}
              </div>
            </div>
            <div className="space-y-3 border-l-2 border-kb-border/50 pl-8 group hover:border-kb-neon/50 transition-colors">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-info transition-colors">
                Heap Allocation
              </div>
              <div className="text-2xl font-black text-kb-info italic tracking-tighter bg-kb-bg/40 px-3 py-1 border border-kb-border/30">
                {system?.memory || "0 MB"}
              </div>
            </div>
            <div className="space-y-3 border-l-2 border-kb-border/50 pl-8 group hover:border-kb-neon/50 transition-colors">
              <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-warning transition-colors">
                System Uptime
              </div>
              <div className="text-2xl font-black text-kb-warning italic tracking-tighter tabular-nums bg-kb-bg/40 px-3 py-1 border border-kb-border/30">
                {formatUptime(uptimeSeconds)}
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  );
};
