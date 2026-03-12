import { AnimatePresence, motion } from "framer-motion";
import {
  Activity,
  Loader2,
  Pause,
  Play,
  Settings,
  Trash2,
  X,
} from "lucide-react";
import { createPortal } from "react-dom";
import { toast } from "sonner";
import { useTopicActions } from "../api/queries";
import { useStore } from "../store/useStore";
import type { Topic } from "../types";
import { TopicConfigDetails } from "./TopicConfigDetails";
import { TopicQueueStatus } from "./TopicQueueStatus";
import { TopicStatsGrid } from "./TopicStatsGrid";

interface TopicDrawerProps {
  topic: Topic | null;
  onClose: () => void;
}

export const TopicDrawer = ({ topic, onClose }: TopicDrawerProps) => {
  const { pause, resume, purge, isPausing, isResuming, isPurging } =
    useTopicActions();
  const { openConfirm } = useStore();

  const handlePauseToggle = () => {
    if (!topic) return;
    const topicName = topic.name;
    if (topic.paused) {
      resume(topicName, {
        onSuccess: () => toast.success(`Topic "${topicName}" resumed`),
        onError: (err) => toast.error(`Resume failed: ${err.message}`),
      });
    } else {
      pause(topicName, {
        onSuccess: () => toast.success(`Topic "${topicName}" paused`),
        onError: (err) => toast.error(`Pause failed: ${err.message}`),
      });
    }
  };

  const handlePurge = () => {
    if (!topic) return;
    const topicName = topic.name;
    openConfirm({
      title: "Purge_Topic_Queue",
      description: `Target Topic: ${topicName}`,
      message: (
        <div className="space-y-2">
          <p>
            This action will{" "}
            <span className="font-bold text-kb-text">PERMANENTLY DELETE</span>{" "}
            all:
          </p>
          <ul className="list-disc list-inside text-sm opacity-80">
            <li>Pending tasks in the main queue</li>
            <li>Delayed tasks scheduled for the future</li>
          </ul>
        </div>
      ),
      variant: "danger",
      onConfirm: () =>
        purge(topicName, {
          onSuccess: () => toast.success(`Queue for "${topicName}" purged`),
          onError: (err) => toast.error(`Purge failed: ${err.message}`),
        }),
    });
  };

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
            className="absolute inset-0 bg-black/80 backdrop-blur-md pointer-events-auto"
          />

          {/* Panel */}
          <motion.div
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%" }}
            transition={{ type: "spring", damping: 30, stiffness: 300 }}
            className="relative w-full max-w-md h-full bg-kb-bg border-l border-kb-border shadow-[0_0_50px_rgba(0,0,0,0.5)] overflow-hidden flex flex-col pointer-events-auto"
          >
            {/* Cyber Header with Glow */}
            <header className="p-8 border-b border-kb-border bg-kb-card relative">
              <div className="absolute top-0 left-0 w-full h-[2px] bg-gradient-to-r from-transparent via-kb-neon to-transparent opacity-50" />
              <div className="flex justify-between items-center">
                <div className="flex items-center gap-4">
                  <motion.div
                    whileHover={{ rotate: 90 }}
                    className="w-12 h-12 bg-kb-bg border border-kb-neon/30 flex items-center justify-center shadow-[0_0_15px_rgba(0,255,159,0.1)]"
                  >
                    <Settings className="text-kb-neon" size={24} />
                  </motion.div>
                  <div>
                    <h2 className="text-2xl font-black italic tracking-tighter uppercase leading-none text-kb-text">
                      Manage Topic
                    </h2>
                    <p className="text-[10px] text-kb-neon font-black tracking-[0.4em] mt-2 uppercase flex items-center gap-2">
                      <span className="w-1.5 h-1.5 bg-kb-neon animate-pulse rounded-full" />
                      {topic.name}
                    </p>
                  </div>
                </div>
                <motion.button
                  whileHover={{ scale: 1.1, rotate: 90 }}
                  whileTap={{ scale: 0.9 }}
                  type="button"
                  onClick={onClose}
                  className="p-3 bg-kb-bg border border-kb-border hover:border-kb-neon hover:text-kb-neon transition-colors"
                >
                  <X size={20} />
                </motion.button>
              </div>
            </header>

            <div className="flex-1 overflow-y-auto p-8 space-y-10 custom-scrollbar">
              {/* Quick Stats Grid */}
              <section className="space-y-4">
                <div className="flex justify-between items-end">
                  <h3 className="text-[10px] font-black uppercase tracking-[0.4em] text-kb-subtext flex items-center gap-2">
                    <Activity size={12} className="text-kb-neon" /> Live Metrics
                  </h3>
                  <div className="h-[1px] flex-1 bg-kb-border ml-4 opacity-30" />
                </div>
                <TopicStatsGrid topic={topic} />
              </section>

              {/* Configuration Details */}
              <section className="space-y-4">
                <div className="flex justify-between items-end">
                  <h3 className="text-[10px] font-black uppercase tracking-[0.4em] text-kb-subtext flex items-center gap-2">
                    <Settings size={12} className="text-kb-neon" />{" "}
                    Configuration
                  </h3>
                  <div className="h-[1px] flex-1 bg-kb-border ml-4 opacity-30" />
                </div>
                <TopicConfigDetails topic={topic} />
              </section>

              {/* Queue Information */}
              <section className="space-y-4">
                <div className="flex justify-between items-end">
                  <h3 className="text-[10px] font-black uppercase tracking-[0.4em] text-kb-subtext">
                    Queue Status_Map
                  </h3>
                  <div className="h-[1px] flex-1 bg-kb-border ml-4 opacity-30" />
                </div>
                <TopicQueueStatus topic={topic} />
              </section>
            </div>

            {/* Actions Footer */}
            <footer className="p-8 border-t border-kb-border bg-kb-card">
              <div className="grid grid-cols-2 gap-4">
                <motion.button
                  whileHover={{ scale: 1.02, brightness: 1.1 }}
                  whileTap={{ scale: 0.98 }}
                  type="button"
                  disabled={isPausing || isResuming}
                  onClick={handlePauseToggle}
                  className={`group relative flex items-center justify-center gap-3 py-4 font-black text-[11px] uppercase italic transition-all overflow-hidden ${
                    topic.paused
                      ? "bg-kb-text text-black"
                      : "bg-kb-neon text-black shadow-[0_0_20px_rgba(0,255,159,0.2)]"
                  } disabled:opacity-50 min-h-[52px]`}
                >
                  <div className="absolute inset-0 bg-white/10 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-500 skew-x-12" />
                  {isPausing || isResuming ? (
                    <Loader2 size={16} className="animate-spin" />
                  ) : topic.paused ? (
                    <Play size={16} fill="currentColor" />
                  ) : (
                    <Pause size={16} fill="currentColor" />
                  )}
                  {isPausing
                    ? "Pausing..."
                    : isResuming
                      ? "Resuming..."
                      : topic.paused
                        ? "Resume_Service"
                        : "Pause_Service"}
                </motion.button>
                <motion.button
                  whileHover={{
                    scale: 1.02,
                    backgroundColor: "rgba(239, 68, 68, 0.1)",
                  }}
                  whileTap={{ scale: 0.98 }}
                  type="button"
                  disabled={isPurging}
                  onClick={handlePurge}
                  className="flex items-center justify-center gap-3 py-4 border border-red-500/30 text-red-500 font-black text-[11px] uppercase hover:border-red-500 transition-colors disabled:opacity-50 min-h-[52px]"
                >
                  {isPurging ? (
                    <Loader2 size={16} className="animate-spin" />
                  ) : (
                    <Trash2 size={16} />
                  )}
                  {isPurging ? "Purging..." : "Purge_Queue"}
                </motion.button>
              </div>
            </footer>
          </motion.div>
        </div>
      )}
    </AnimatePresence>
  );

  return createPortal(content, document.body);
};
