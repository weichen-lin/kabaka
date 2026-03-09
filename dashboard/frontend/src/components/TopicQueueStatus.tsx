import { motion } from "framer-motion";
import type { Topic } from "../types";

interface TopicQueueStatusProps {
  topic: Topic;
}

export const TopicQueueStatus = ({ topic }: TopicQueueStatusProps) => {
  const items = [
    {
      label: "Pending",
      value: topic.queue_pending,
      color: "bg-kb-neon",
      sub: "QUEUE_WAIT",
    },
    {
      label: "Delayed",
      value: topic.queue_delayed,
      color: "bg-kb-warning",
      sub: "SCHED_DELAY",
    },
    {
      label: "Processing",
      value: topic.queue_processing,
      color: "bg-kb-info",
      sub: "ACTIVE_JOB",
    },
  ];

  return (
    <div className="bg-kb-card border border-kb-border divide-y divide-kb-border">
      {items.map((item, i) => (
        <motion.div
          key={item.label}
          initial={{ opacity: 0, x: -10 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: i * 0.1 }}
          className="flex justify-between items-center p-4 group hover:bg-kb-bg transition-colors"
        >
          <div className="flex flex-col">
            <span className="text-[10px] font-black uppercase tracking-widest text-kb-subtext group-hover:text-kb-text transition-colors">
              {item.label}
            </span>
            <span className="text-[7px] font-bold text-kb-subtext/40 tracking-[0.3em] uppercase">
              {item.sub}
            </span>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right mr-2">
              <span className="text-lg font-black font-mono leading-none">
                {item.value}
              </span>
            </div>
            <div className="relative">
              <div
                className={`h-2 w-2 rounded-full ${item.color} animate-pulse relative z-10`}
              />
              <div
                className={`absolute inset-0 h-2 w-2 rounded-full ${item.color} animate-ping opacity-30`}
              />
            </div>
          </div>
        </motion.div>
      ))}
    </div>
  );
};
