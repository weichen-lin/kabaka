import { Link } from "@tanstack/react-router";
import { AnimatePresence, motion } from "framer-motion";
import { Database, Filter, RefreshCw, Search } from "lucide-react";
import { useState } from "react";
import { type Topic, useTopics } from "../api/queries";
import { StatusTag } from "./StatusTag";

const formatDuration = (ms: number) => {
  if (ms < 1000) return `${ms}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = seconds / 60;
  if (minutes < 60) return `${minutes.toFixed(1)}m`;
  const hours = minutes / 60;
  return `${hours.toFixed(1)}h`;
};

export const Topics = () => {
  const { data, isLoading, refetch, isRefetching } = useTopics();
  const [filter, setFilter] = useState("");

  const filteredTopics =
    data?.topics.filter(
      (topic: Topic) =>
        topic.name.toLowerCase().includes(filter.toLowerCase()) ||
        topic.internal_name.toLowerCase().includes(filter.toLowerCase()),
    ) || [];

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden p-8 space-y-6 relative z-10">
      <div className="flex-none space-y-6">
        <header className="flex justify-between items-end">
          <div>
            <h2 className="text-3xl font-black italic tracking-tighter text-kb-text uppercase leading-none">
              Topic Registry
            </h2>
            <p className="text-[10px] text-kb-subtext tracking-[0.3em] font-black mt-1.5 uppercase opacity-80">
              Full cluster topic synchronization map
            </p>
          </div>
          <div className="flex gap-3">
            <button
              type="button"
              onClick={() => refetch()}
              className="p-2 border border-kb-border bg-kb-card hover:border-kb-neon transition-colors group"
            >
              <RefreshCw
                size={16}
                className={`text-kb-subtext group-hover:text-kb-neon ${isRefetching ? "animate-spin" : ""}`}
              />
            </button>
          </div>
        </header>

        {/* Filter Bar */}
        <div className="flex gap-4 items-center bg-kb-card border border-kb-border p-3 focus-within:border-kb-neon/50 transition-colors">
          <div className="flex-1 flex items-center gap-3 px-3">
            <Search size={16} className="text-kb-subtext" />
            <input
              type="text"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="FILTER BY TOPIC NAME..."
              className="bg-transparent border-none outline-none text-xs font-bold tracking-widest text-kb-text w-full placeholder:text-kb-subtext/40 uppercase"
            />
          </div>
          <div className="h-6 w-[1px] bg-kb-border" />
          <button
            type="button"
            className="flex items-center gap-2 px-4 text-[10px] font-black uppercase text-kb-subtext hover:text-kb-neon transition-colors"
          >
            <Filter size={14} />
            Filters
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto min-h-0 pr-2 custom-scrollbar">
        <div className="grid grid-cols-1 gap-4">
          {isLoading ? (
            <div className="py-20 text-center text-kb-neon animate-pulse font-black uppercase tracking-[0.5em]">
              Syncing Registry...
            </div>
          ) : (
            <AnimatePresence mode="popLayout">
              {filteredTopics.map((topic: Topic) => (
                <motion.div
                  key={topic.name}
                  layout
                  initial={{ opacity: 0, scale: 0.98 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.98 }}
                  className="bg-kb-card border border-kb-border group hover:border-kb-neon/50 transition-colors"
                >
                  <div className="p-6 flex flex-wrap lg:flex-nowrap items-center gap-8">
                    <div className="flex items-center gap-4 w-full lg:w-1/3">
                      <div
                        className={`w-10 h-10 bg-kb-bg border border-kb-border flex items-center justify-center group-hover:border-kb-neon transition-colors ${topic.paused ? "border-kb-warning/30" : ""}`}
                      >
                        <Database
                          size={18}
                          className={`text-kb-subtext group-hover:text-kb-neon ${topic.paused ? "text-kb-warning" : ""}`}
                        />
                      </div>
                      <div>
                        <h3 className="font-black text-lg italic tracking-tight uppercase group-hover:text-kb-neon transition-colors">
                          {topic.name}
                        </h3>
                        <div className="flex items-center gap-2 mt-1">
                          <StatusTag
                            label={topic.paused ? "Paused" : "Active"}
                            status={topic.paused ? "paused" : "ok"}
                          />
                        </div>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-4 gap-8 flex-1">
                      <div className="space-y-1">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Processed
                        </div>
                        <div className="text-sm font-black text-kb-text">
                          {topic.processed_total.toLocaleString()}
                        </div>
                      </div>
                      <div className="space-y-1">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Success Rate
                        </div>
                        <div className="text-sm font-black text-kb-neon">
                          {topic.success_rate}%
                        </div>
                      </div>
                      <div className="space-y-1">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Avg Duration
                        </div>
                        <div className="text-sm font-black text-kb-info">
                          {formatDuration(topic.avg_duration)}
                        </div>
                      </div>
                      <div className="space-y-1">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Pending
                        </div>
                        <div className="text-sm font-black text-kb-warning">
                          {topic.queue_pending ?? 0}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-2">
                      <Link
                        to="/topics/$internalName"
                        params={{ internalName: topic.internal_name }}
                        className="px-6 py-2 border border-kb-border text-[9px] font-black uppercase tracking-widest hover:bg-kb-neon hover:text-black hover:border-kb-neon transition-all"
                      >
                        Manage
                      </Link>
                    </div>
                  </div>

                  {/* Mini Sparkline placeholder */}
                  <div className="h-1 w-full bg-kb-bg relative overflow-hidden">
                    <motion.div
                      initial={{ x: "-100%" }}
                      animate={{ x: "100%" }}
                      transition={{
                        duration: 3,
                        repeat: Infinity,
                        ease: "linear",
                      }}
                      className="absolute inset-0 bg-gradient-to-r from-transparent via-kb-neon/20 to-transparent w-1/2"
                    />
                  </div>
                </motion.div>
              ))}
              {filteredTopics.length === 0 && (
                <div className="py-20 text-center text-kb-subtext uppercase text-[10px] font-black tracking-widest italic opacity-50">
                  No topics matching "{filter}"
                </div>
              )}
            </AnimatePresence>
          )}
        </div>
      </div>
    </div>
  );
};
