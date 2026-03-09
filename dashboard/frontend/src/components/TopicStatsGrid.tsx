import { motion } from "framer-motion";
import type { Topic } from "../types";
import { formatDuration } from "../utils/format";

interface TopicStatsGridProps {
  topic: Topic;
}

export const TopicStatsGrid = ({ topic }: TopicStatsGridProps) => {
  const stats = [
    {
      label: "Processed",
      value: topic.processed_total.toLocaleString(),
      color: "text-kb-text",
      sub: "TOTAL_JOBS",
    },
    {
      label: "Failed",
      value: topic.failed_total.toLocaleString(),
      color: "text-red-500",
      sub: "ERR_COUNT",
    },
    {
      label: "Avg Duration",
      value: formatDuration(topic.avg_duration),
      color: "text-kb-info",
      sub: "LATENCY_MS",
    },
    {
      label: "Success Rate",
      value: `${topic.success_rate}%`,
      color: "text-kb-neon",
      sub: "THR_PUT",
    },
  ];

  return (
    <div className="grid grid-cols-2 gap-4">
      {stats.map((stat, i) => (
        <motion.div
          key={stat.label}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: i * 0.05 }}
          whileHover={{ scale: 1.02 }}
          className="bg-kb-card border border-kb-border p-4 relative group overflow-hidden"
        >
          {/* Glitch Effect Background */}
          <div className="absolute inset-0 bg-kb-neon/5 opacity-0 group-hover:opacity-100 transition-opacity duration-300 pointer-events-none" />

          <div className="flex justify-between items-start mb-1">
            <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
              {stat.label}
            </div>
            <div className="text-[7px] font-black text-kb-neon/30 uppercase tracking-[0.2em] italic">
              {stat.sub}
            </div>
          </div>
          <div
            className={`text-xl font-black ${stat.color} font-mono tracking-tighter`}
          >
            {stat.value}
          </div>

          {/* Bottom Accent */}
          <div className="absolute bottom-0 left-0 h-[1px] bg-kb-neon/30 w-0 group-hover:w-full transition-all duration-500" />
        </motion.div>
      ))}
    </div>
  );
};
