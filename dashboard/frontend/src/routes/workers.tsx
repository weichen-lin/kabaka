import { createFileRoute } from "@tanstack/react-router";
import { AnimatePresence, motion } from "framer-motion";
import { Activity, Cpu, LayoutGrid, Shield, Terminal, Zap } from "lucide-react";
import { useStats } from "../api/queries";

export const Route = createFileRoute("/workers")({
  component: WorkersFleet,
});

function WorkersFleet() {
  const { data, isLoading } = useStats();

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="text-kb-neon animate-pulse font-black uppercase tracking-[0.5em] italic text-sm">
          Initializing node fleet...
        </div>
      </div>
    );
  }

  const activeJobs = data?.stats.active_jobs || 0;
  const idleSlots = data?.stats.idle_slots || 0;
  const totalSlots = activeJobs + idleSlots;

  // Create an array representing all worker slots with stable IDs
  const slots = [
    ...Array(activeJobs)
      .fill(0)
      .map((_, i) => ({ id: `active-${i}`, status: "active" as const })),
    ...Array(idleSlots)
      .fill(0)
      .map((_, i) => ({ id: `idle-${i}`, status: "idle" as const })),
  ];

  return (
    <div className="flex-1 h-full overflow-y-auto p-8 space-y-8 relative z-10 custom-scrollbar">
      <header className="flex justify-between items-end">
        <div>
          <h2 className="text-3xl font-black italic tracking-tighter text-kb-text uppercase leading-none">
            Worker Fleet Allocation
          </h2>
          <p className="text-[10px] text-kb-subtext tracking-[0.3em] font-black mt-1.5 uppercase opacity-80">
            Real-time cluster processing distribution
          </p>
        </div>
        <div className="flex items-center gap-4 bg-kb-card border border-kb-border px-4 py-2">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-kb-neon animate-pulse shadow-[0_0_8px_var(--kb-neon)]" />
            <span className="text-[10px] font-black uppercase text-kb-text">
              Active: {activeJobs}
            </span>
          </div>
          <div className="w-[1px] h-3 bg-kb-border" />
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-kb-subtext/30" />
            <span className="text-[10px] font-black uppercase text-kb-subtext">
              Idle: {idleSlots}
            </span>
          </div>
        </div>
      </header>

      {/* Grid of Workers */}
      <section className="space-y-4">
        <div className="flex items-center gap-3">
          <LayoutGrid size={14} className="text-kb-subtext" />
          <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext">
            Fleet Topography
          </h3>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-3">
          <AnimatePresence mode="popLayout">
            {slots.map((slot, idx) => (
              <motion.div
                key={slot.id}
                layout
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                className={`relative aspect-square border transition-all duration-500 group overflow-hidden ${
                  slot.status === "active"
                    ? "bg-kb-neon/10 border-kb-neon shadow-[inset_0_0_20px_rgba(var(--kb-neon),0.1)]"
                    : "bg-kb-card/30 border-kb-border hover:border-kb-neon/50 hover:bg-kb-card/50"
                }`}
              >
                {/* Visual content for Active Workers */}
                {slot.status === "active" ? (
                  <div className="absolute inset-0 flex flex-col items-center justify-center p-2">
                    <Zap
                      size={20}
                      className="text-kb-neon animate-pulse mb-1"
                    />
                    <span className="text-[8px] font-black text-kb-neon uppercase tracking-tighter">
                      Working
                    </span>
                    {/* Activity Line */}
                    <div className="absolute bottom-2 left-2 right-2 h-0.5 bg-kb-neon/20 overflow-hidden">
                      <motion.div
                        animate={{ x: ["-100%", "100%"] }}
                        transition={{
                          duration: 1.5,
                          repeat: Infinity,
                          ease: "linear",
                        }}
                        className="w-1/2 h-full bg-kb-neon shadow-[0_0_5px_var(--kb-neon)]"
                      />
                    </div>
                  </div>
                ) : (
                  /* Visual content for Idle Workers - Enhanced for clarity */
                  <div className="absolute inset-0 flex flex-col items-center justify-center p-2 opacity-60 group-hover:opacity-100 transition-opacity">
                    <div className="w-6 h-6 rounded-full border border-kb-subtext flex items-center justify-center">
                      <div className="w-1.5 h-1.5 bg-kb-subtext/50 rounded-full" />
                    </div>
                    <span className="text-[8px] font-black text-kb-subtext uppercase tracking-tighter mt-1.5">
                      Standby
                    </span>
                  </div>
                )}

                {/* Slot Identifier */}
                <div className="absolute top-1 right-1 text-[7px] font-mono text-kb-subtext/70">
                  W_{(idx + 1).toString().padStart(3, "0")}
                </div>
              </motion.div>
            ))}
          </AnimatePresence>

          {/* Add placeholders if few workers exist to maintain grid structure visually */}
          {totalSlots < 16 &&
            Array(16 - totalSlots)
              .fill(0)
              .map((_, idx) => (
                <div
                  // biome-ignore lint/suspicious/noArrayIndexKey: Empty placeholder slots have stable order
                  key={`empty-${totalSlots + idx}`}
                  className="aspect-square border border-dashed border-kb-border/40 bg-kb-bg/50 flex items-center justify-center"
                >
                  <div className="text-[8px] font-black text-kb-subtext/20 uppercase tracking-widest">
                    Empty
                  </div>
                </div>
              ))}
        </div>
      </section>

      {/* System Resource Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-kb-card border border-kb-border p-6 space-y-4">
          <div className="flex items-center gap-3 text-kb-subtext">
            <Cpu size={16} />
            <span className="text-[10px] font-black uppercase tracking-widest">
              Compute Efficiency
            </span>
          </div>
          <div className="space-y-2">
            <div className="flex justify-between text-xs font-bold uppercase italic">
              <span className="text-kb-subtext">Utilization</span>
              <span className="text-kb-neon">
                {totalSlots > 0
                  ? Math.round((activeJobs / totalSlots) * 100)
                  : 0}
                %
              </span>
            </div>
            <div className="h-2 bg-kb-bg border border-kb-border overflow-hidden p-[1px]">
              <motion.div
                initial={{ width: 0 }}
                animate={{
                  width: `${totalSlots > 0 ? (activeJobs / totalSlots) * 100 : 0}%`,
                }}
                className="h-full bg-kb-neon"
              />
            </div>
          </div>
        </div>

        <div className="bg-kb-card border border-kb-border p-6 space-y-4">
          <div className="flex items-center gap-3 text-kb-subtext">
            <Shield size={16} />
            <span className="text-[10px] font-black uppercase tracking-widest">
              Health Status
            </span>
          </div>
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-full border-2 border-kb-neon flex items-center justify-center">
              <Activity size={24} className="text-kb-neon" />
            </div>
            <div>
              <div className="text-sm font-black text-kb-text uppercase">
                Fleet Optimal
              </div>
              <div className="text-[9px] font-bold text-kb-neon uppercase tracking-tighter">
                All Systems Operational
              </div>
            </div>
          </div>
        </div>

        <div className="bg-kb-card border border-kb-border p-6 space-y-4">
          <div className="flex items-center gap-3 text-kb-subtext">
            <Terminal size={16} />
            <span className="text-[10px] font-black uppercase tracking-widest">
              Runtime Context
            </span>
          </div>
          <div className="space-y-1 font-mono text-[10px]">
            <div className="flex justify-between">
              <span className="text-kb-subtext">Go Version</span>
              <span className="text-kb-text">{data?.system.go_version}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-kb-subtext">Goroutines</span>
              <span className="text-kb-text">{data?.system.goroutines}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-kb-subtext">Memory Usage</span>
              <span className="text-kb-text">{data?.system.memory}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
