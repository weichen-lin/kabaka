import { AnimatePresence, motion } from "framer-motion";
import {
  Activity,
  Clock,
  Database,
  Pause,
  RotateCcw,
  Settings,
  ShieldCheck,
  Trash2,
  X,
} from "lucide-react";
import { createPortal } from "react-dom";
import type { Topic } from "../api/queries";

interface TopicDrawerProps {
  topic: Topic | null;
  onClose: () => void;
}

const formatDuration = (ms: number) => {
  if (ms < 1000) return `${ms}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = seconds / 60;
  if (minutes < 60) return `${minutes.toFixed(1)}m`;
  const hours = minutes / 60;
  return `${hours.toFixed(1)}h`;
};

export const TopicDrawer = ({ topic, onClose }: TopicDrawerProps) => {
  const content = (
    <AnimatePresence>
      {topic && (
        <div className="fixed inset-0 z-[9999] flex justify-end pointer-events-none">
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="absolute inset-0 bg-black/60 backdrop-blur-sm pointer-events-auto"
          />

          {/* Panel */}
          <motion.div
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%" }}
            transition={{ type: "spring", damping: 25, stiffness: 200 }}
            className="relative w-full max-w-md h-full bg-kb-bg border-l border-kb-border shadow-2xl overflow-hidden flex flex-col pointer-events-auto"
          >
            {/* Header */}
            <header className="p-6 border-b border-kb-border bg-kb-card flex justify-between items-center">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-kb-bg border border-kb-neon/30 flex items-center justify-center">
                  <Settings className="text-kb-neon" size={20} />
                </div>
                <div>
                  <h2 className="text-xl font-black italic tracking-tight uppercase leading-none">
                    Manage Topic
                  </h2>
                  <p className="text-[10px] text-kb-neon font-black tracking-widest mt-1">
                    {topic.name}
                  </p>
                </div>
              </div>
              <button
                type="button"
                onClick={onClose}
                className="p-2 hover:bg-kb-neon hover:text-black transition-colors"
              >
                <X size={20} />
              </button>
            </header>

            <div className="flex-1 overflow-y-auto p-6 space-y-8">
              {/* Quick Stats Grid */}
              <section className="space-y-4">
                <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
                  <Activity size={12} /> Live Metrics
                </h3>
                <div className="grid grid-cols-2 gap-4">
                  {topic &&
                    [
                      {
                        label: "Processed",
                        value: topic.processed_total.toLocaleString(),
                        color: "text-kb-text",
                      },
                      {
                        label: "Failed",
                        value: topic.failed_total.toLocaleString(),
                        color: "text-red-500",
                      },
                      {
                        label: "Avg Duration",
                        value: formatDuration(topic.avg_duration),
                        color: "text-kb-info",
                      },
                      {
                        label: "Success Rate",
                        value: `${topic.success_rate}%`,
                        color: "text-kb-neon",
                      },
                    ].map((stat) => (
                      <div
                        key={stat.label}
                        className="bg-kb-card border border-kb-border p-4"
                      >
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest mb-1">
                          {stat.label}
                        </div>
                        <div className={`text-xl font-black ${stat.color}`}>
                          {stat.value}
                        </div>
                      </div>
                    ))}
                </div>
              </section>

              {/* Configuration Details */}
              <section className="space-y-4">
                <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
                  <Settings size={12} /> Configuration
                </h3>
                <div className="space-y-2">
                  {[
                    {
                      label: "Internal Name",
                      value: topic.internal_name,
                      icon: <Database size={14} />,
                    },
                    {
                      label: "Max Retries",
                      value: `${topic.max_retries} attempts`,
                      icon: <RotateCcw size={14} />,
                    },
                    {
                      label: "Retry Delay",
                      value: `${topic.retry_delay}s`,
                      icon: <Clock size={14} />,
                    },
                    {
                      label: "Process Timeout",
                      value: `${topic.process_timeout}s`,
                      icon: <ShieldCheck size={14} />,
                    },
                  ].map((item) => (
                    <div
                      key={item.label}
                      className="flex items-center justify-between p-3 bg-kb-card/50 border border-kb-border group hover:border-kb-neon/30 transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <div className="text-kb-subtext group-hover:text-kb-neon transition-colors">
                          {item.icon}
                        </div>
                        <span className="text-[10px] font-black uppercase tracking-widest text-kb-subtext">
                          {item.label}
                        </span>
                      </div>
                      <span className="text-xs font-bold text-kb-text font-mono truncate max-w-[180px]">
                        {item.value}
                      </span>
                    </div>
                  ))}
                </div>
              </section>

              {/* Queue Information */}
              <section className="space-y-4">
                <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext">
                  Queue Status
                </h3>
                <div className="bg-kb-card border border-kb-border divide-y divide-kb-border">
                  {Object.entries(topic.queue_stats || {}).map(
                    ([key, value]) => (
                      <div
                        key={key}
                        className="flex justify-between items-center p-4"
                      >
                        <span className="text-[10px] font-black uppercase tracking-widest text-kb-subtext">
                          {key}
                        </span>
                        <div className="flex items-center gap-2">
                          <div className="h-1.5 w-1.5 rounded-full bg-kb-neon animate-pulse" />
                          <span className="text-sm font-black">{value}</span>
                        </div>
                      </div>
                    ),
                  )}
                </div>
              </section>
            </div>

            {/* Actions Footer */}
            <footer className="p-6 border-t border-kb-border bg-kb-card space-y-3">
              <button
                type="button"
                className="w-full flex items-center justify-center gap-2 py-3 bg-kb-neon text-black font-black text-xs uppercase italic hover:brightness-110 transition-all"
              >
                <Pause size={16} />
                Pause Processing
              </button>
              <div className="grid grid-cols-2 gap-3">
                <button
                  type="button"
                  className="flex items-center justify-center gap-2 py-3 border border-kb-border font-black text-[10px] uppercase hover:bg-kb-text/5 transition-colors"
                >
                  Clear Stats
                </button>
                <button
                  type="button"
                  className="flex items-center justify-center gap-2 py-3 border border-red-500/30 text-red-500 font-black text-[10px] uppercase hover:bg-red-500/10 transition-colors"
                >
                  <Trash2 size={14} />
                  Purge Topic
                </button>
              </div>
            </footer>
          </motion.div>
        </div>
      )}
    </AnimatePresence>
  );

  return createPortal(content, document.body);
};
