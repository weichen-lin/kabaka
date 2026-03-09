import { Clock, Database, RotateCcw, ShieldCheck } from "lucide-react";
import type { Topic } from "../types";

interface TopicConfigDetailsProps {
  topic: Topic;
}

export const TopicConfigDetails = ({ topic }: TopicConfigDetailsProps) => {
  const configItems = [
    {
      label: "Internal Name",
      value: topic.internal_name,
      icon: <Database size={14} />,
      sub: "INTERNAL_ID",
    },
    {
      label: "Max Retries",
      value: `${topic.max_retries} attempts`,
      icon: <RotateCcw size={14} />,
      sub: "RETRY_LIMIT",
    },
    {
      label: "Retry Delay",
      value: `${topic.retry_delay}s`,
      icon: <Clock size={14} />,
      sub: "RETRY_WAIT",
    },
    {
      label: "Process Timeout",
      value: `${topic.process_timeout}s`,
      icon: <ShieldCheck size={14} />,
      sub: "TIMEOUT_SEC",
    },
  ];

  return (
    <div className="space-y-2">
      {configItems.map((item) => (
        <div
          key={item.label}
          className="flex items-center justify-between p-3 bg-kb-card/50 border border-kb-border group hover:border-kb-neon/30 transition-colors relative overflow-hidden"
        >
          {/* Subtle Side Glow */}
          <div className="absolute left-0 top-0 bottom-0 w-[2px] bg-kb-neon opacity-0 group-hover:opacity-100 transition-opacity duration-300" />

          <div className="flex items-center gap-3">
            <div className="text-kb-subtext group-hover:text-kb-neon transition-colors">
              {item.icon}
            </div>
            <div className="flex flex-col">
              <span className="text-[10px] font-black uppercase tracking-widest text-kb-subtext group-hover:text-kb-text transition-colors">
                {item.label}
              </span>
              <span className="text-[6px] font-bold text-kb-subtext/30 tracking-[0.4em] uppercase">
                {item.sub}
              </span>
            </div>
          </div>
          <span className="text-xs font-bold text-kb-text font-mono truncate max-w-[180px] bg-kb-bg/50 px-2 py-1 border border-kb-border/50">
            {item.value}
          </span>
        </div>
      ))}
    </div>
  );
};
