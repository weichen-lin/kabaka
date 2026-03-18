import { motion } from "framer-motion";
import {
  Activity,
  Cpu,
  Globe,
  HardDrive,
  Server,
  ShieldAlert,
} from "lucide-react";
import { useInstances, useStats } from "../api/queries";
import { formatUptime } from "../utils/format";
import { InstancesOverview } from "./InstancesOverview";
import { StatCard } from "./StatCard";

export const Dashboard = () => {
  const { data: statsData, isLoading } = useStats();
  const { data: instancesData } = useInstances();

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

  const showInstances = instancesData?.supported;

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden p-6 space-y-6 relative z-10">
      <div className="flex-none space-y-6">
        <header className="flex justify-between items-end border-b border-kb-border pb-4 min-h-[54px]">
          <div>
            <h2 className="text-3xl font-black italic tracking-tighter text-kb-text uppercase leading-none">
              System Overview
            </h2>
          </div>
        </header>

        {/* Aggregate Stats */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
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
      </div>

      {/* Middle: Worker Fleet + Diagnostics | Instances */}
      <div
        className={`flex-1 min-h-0 grid gap-4 ${showInstances ? "grid-cols-1 lg:grid-cols-5" : "grid-cols-1"}`}
      >
        {/* Left column: Worker Fleet + System Diagnostics stacked */}
        <div
          className={`flex flex-col gap-4 min-h-0 ${showInstances ? "lg:col-span-3" : ""}`}
        >
          {/* Worker Fleet Monitor */}
          <section className="bg-kb-card border border-kb-border p-5 relative overflow-hidden flex flex-col group shrink-0">
            <div className="absolute top-0 left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-kb-neon/20 to-transparent" />

            <div className="flex justify-between items-center mb-4 shrink-0">
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-kb-bg border border-kb-neon/20 flex items-center justify-center group-hover:border-kb-neon/50 transition-colors">
                  <Server size={16} className="text-kb-neon" />
                </div>
                <h3 className="text-xs font-black uppercase tracking-[0.3em] flex flex-col">
                  Worker Fleet Monitor
                  <span className="text-[8px] text-kb-subtext/50 tracking-widest mt-0.5 italic">
                    REAL TIME NODE ALLOCATION
                  </span>
                </h3>
              </div>

              <div className="flex items-center gap-6 bg-kb-bg/50 px-4 py-2 border border-kb-border">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-sm bg-kb-neon shadow-[0_0_10px_rgba(0,255,159,0.4)]" />
                  <span className="text-[9px] font-black text-kb-text uppercase tracking-widest leading-none">
                    ACTIVE: {stats?.active_jobs || 0}
                  </span>
                </div>
                <div className="w-[1px] h-3 bg-kb-border" />
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-sm bg-kb-border" />
                  <span className="text-[9px] font-black text-kb-subtext uppercase tracking-widest leading-none">
                    IDLE: {stats?.idle_slots || 0}
                  </span>
                </div>
              </div>
            </div>

            <div className="overflow-hidden grid grid-cols-10 sm:grid-cols-20 md:grid-cols-40 lg:grid-cols-50 gap-1.5 relative z-10 p-3 bg-kb-bg/20 border border-kb-border/30 auto-rows-[16px] content-start">
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
                  className="h-4 w-full min-w-[5px] border border-kb-border/50 relative overflow-hidden"
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

          {/* System Runtime Diagnostics */}
          <section className="bg-kb-card border border-kb-border p-5 relative overflow-hidden shrink-0">
            <div className="absolute top-0 right-0 p-8 opacity-5">
              <Globe
                size={160}
                className="text-kb-info animate-[spin_60s_linear_infinite]"
              />
            </div>

            <div className="flex items-center gap-4 mb-4">
              <div className="w-8 h-8 bg-kb-bg border border-kb-info/20 flex items-center justify-center">
                <HardDrive size={16} className="text-kb-info" />
              </div>
              <h3 className="text-xs font-black uppercase tracking-[0.3em] flex flex-col">
                System Runtime Diagnostics
                <span className="text-[8px] text-kb-subtext/50 tracking-widest mt-0.5 italic">
                  CORE ENVIRONMENT
                </span>
              </h3>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-8 relative z-10">
              <div className="space-y-2 border-l-2 border-kb-border/50 pl-6 group hover:border-kb-neon/50 transition-colors">
                <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-text transition-colors">
                  Go Runtime
                </div>
                <div className="text-xl font-black text-kb-text italic tracking-tighter bg-kb-bg/40 px-2 py-0.5 border border-kb-border/30">
                  {system?.go_version?.split(" ")[0] || "N/A"}
                </div>
              </div>
              <div className="space-y-2 border-l-2 border-kb-border/50 pl-6 group hover:border-kb-neon/50 transition-colors">
                <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-neon transition-colors">
                  Active Goroutines
                </div>
                <div className="text-xl font-black text-kb-neon italic tracking-tighter bg-kb-bg/40 px-2 py-0.5 border border-kb-border/30 shadow-[0_0_15px_rgba(0,255,159,0.1)]">
                  {system?.goroutines || 0}
                </div>
              </div>
              <div className="space-y-2 border-l-2 border-kb-border/50 pl-6 group hover:border-kb-neon/50 transition-colors">
                <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-info transition-colors">
                  Heap Allocation
                </div>
                <div className="text-xl font-black text-kb-info italic tracking-tighter bg-kb-bg/40 px-2 py-0.5 border border-kb-border/30">
                  {system?.memory || "0 MB"}
                </div>
              </div>
              <div className="space-y-2 border-l-2 border-kb-border/50 pl-6 group hover:border-kb-neon/50 transition-colors">
                <div className="text-[9px] font-black text-kb-subtext uppercase tracking-[0.3em] group-hover:text-kb-warning transition-colors">
                  System Uptime
                </div>
                <div className="text-xl font-black text-kb-warning italic tracking-tighter tabular-nums bg-kb-bg/40 px-2 py-0.5 border border-kb-border/30">
                  {formatUptime(uptimeSeconds)}
                </div>
              </div>
            </div>
          </section>
        </div>

        {/* Instances Fleet */}
        {showInstances && (
          <div className="lg:col-span-2 min-h-0 overflow-hidden">
            <InstancesOverview instances={instancesData.instances} />
          </div>
        )}
      </div>
    </div>
  );
};
