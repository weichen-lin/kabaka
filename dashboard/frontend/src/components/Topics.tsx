import { Link } from "@tanstack/react-router";
import { AnimatePresence, motion } from "framer-motion";
import { Database, Filter, RefreshCw, Search } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import { type Topic, useTopics } from "../api/queries";
import { StatusTag } from "./StatusTag";

export const Topics = () => {
  const { data, isLoading, refetch, isRefetching } = useTopics();
  const [filter, setFilter] = useState("");

  const handleRefetch = async () => {
    try {
      await refetch();
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      toast.error(`Registry sync failed: ${message}`);
    }
  };

  const filteredTopics = (data?.topics || [])
    .filter(
      (topic: Topic) =>
        topic.name.toLowerCase().includes(filter.toLowerCase()) ||
        topic.internal_name.toLowerCase().includes(filter.toLowerCase()),
    )
    .sort((a, b) => a.name.localeCompare(b.name));

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden p-6 space-y-6 relative z-10">
      <div className="flex-none space-y-6">
        <header className="flex justify-between items-end border-b border-kb-border pb-4">
          <div>
            <h2 className="text-3xl font-black italic tracking-tighter text-kb-text uppercase leading-none">
              Topic Registry
            </h2>
          </div>
          <div className="flex gap-2">
            <motion.button
              whileHover={{ scale: 1.05, rotate: 180 }}
              whileTap={{ scale: 0.95 }}
              type="button"
              onClick={handleRefetch}
              className="p-2.5 border border-kb-border bg-kb-card hover:border-kb-neon transition-colors group shadow-lg"
            >
              <RefreshCw
                size={16}
                className={`text-kb-subtext group-hover:text-kb-neon ${isRefetching ? "animate-spin" : ""}`}
              />
            </motion.button>
          </div>
        </header>

        {/* Filter Bar */}
        <div className="flex gap-4 items-center bg-kb-card border border-kb-border p-2.5 focus-within:border-kb-neon/50 transition-all shadow-inner group">
          <div className="flex-1 flex items-center gap-3 px-3">
            <Search
              size={16}
              className="text-kb-subtext group-focus-within:text-kb-neon transition-colors"
            />
            <input
              type="text"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Search by topic name..."
              className="bg-transparent border-none outline-none text-[11px] font-black tracking-[0.1em] text-kb-text w-full placeholder:text-kb-subtext/40 uppercase"
            />
          </div>
          <div className="h-6 w-[1px] bg-kb-border" />
          <motion.button
            whileHover={{ x: 2 }}
            type="button"
            className="flex items-center gap-2 px-4 text-[9px] font-black uppercase text-kb-subtext hover:text-kb-neon transition-colors"
          >
            <Filter size={14} />
            Filter Topics
          </motion.button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto min-h-0 pr-2 custom-scrollbar">
        <div className="grid grid-cols-1 gap-4">
          {isLoading ? (
            <div className="py-20 flex flex-col items-center justify-center gap-4">
              <div className="w-10 h-10 border-2 border-kb-neon border-t-transparent animate-spin" />
              <div className="text-kb-neon font-black uppercase tracking-[0.6em] text-[10px]">
                Syncing Registry...
              </div>
            </div>
          ) : (
            <AnimatePresence mode="popLayout">
              {filteredTopics.map((topic: Topic, idx: number) => (
                <motion.div
                  key={topic.name}
                  layout
                  initial={{ opacity: 0, y: 15 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.98 }}
                  transition={{ delay: idx * 0.05 }}
                  className="bg-kb-card border border-kb-border group hover:border-kb-neon/50 transition-all relative overflow-hidden"
                >
                  {/* Subtle Background Text */}
                  <div className="absolute -right-4 -top-6 text-[60px] font-black text-kb-text/5 italic pointer-events-none select-none uppercase tracking-tighter">
                    {topic.name}
                  </div>

                  <div className="p-5 flex flex-wrap lg:flex-nowrap items-center gap-6 relative z-10">
                    <div className="flex items-center gap-4 w-full lg:w-1/3">
                      <motion.div
                        whileHover={{ scale: 1.1, rotate: 5 }}
                        className={`w-12 h-12 bg-kb-bg border border-kb-border flex items-center justify-center group-hover:border-kb-neon transition-colors relative ${topic.paused ? "border-kb-warning/50" : ""}`}
                      >
                        {topic.paused && (
                          <div className="absolute inset-0 bg-kb-warning/5 animate-pulse" />
                        )}
                        <Database
                          size={20}
                          className={`text-kb-subtext group-hover:text-kb-neon transition-colors ${topic.paused ? "text-kb-warning" : ""}`}
                        />
                      </motion.div>
                      <div>
                        <h3 className="font-black text-xl italic tracking-tighter uppercase group-hover:text-kb-neon transition-colors leading-none">
                          {topic.name}
                        </h3>
                        <div className="flex items-center gap-2.5 mt-2">
                          <StatusTag
                            label={topic.paused ? "Paused" : "Active"}
                            status={topic.paused ? "paused" : "ok"}
                          />
                          <span className="text-[7px] font-bold text-kb-subtext/40 tracking-widest uppercase italic">
                            {topic.internal_name}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-4 gap-6 flex-1">
                      <div className="space-y-1.5 border-l border-kb-border/30 pl-4 group-hover:border-kb-neon/30 transition-colors">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Processed
                        </div>
                        <div className="text-base font-black text-kb-text font-mono">
                          {topic.processed_total.toLocaleString()}
                        </div>
                      </div>
                      <div className="space-y-1.5 border-l border-kb-border/30 pl-4 group-hover:border-kb-neon/30 transition-colors">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Success Rate
                        </div>
                        <div className="text-base font-black text-kb-neon font-mono">
                          {topic.success_rate !== null
                            ? `${topic.success_rate}%`
                            : "--"}
                        </div>
                      </div>
                      <div className="space-y-1.5 border-l border-kb-border/30 pl-4 group-hover:border-kb-neon/30 transition-colors">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Avg Duration
                        </div>
                        <div className="text-base font-black text-kb-info font-mono">
                          {topic.avg_duration !== null
                            ? `${topic.avg_duration}ms`
                            : "--"}
                        </div>
                      </div>
                      <div className="space-y-1.5 border-l border-kb-border/30 pl-4 group-hover:border-kb-neon/30 transition-colors">
                        <div className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                          Avg Duration
                        </div>
                        <div className="text-base font-black text-kb-info font-mono">
                          {topic.avg_duration !== null
                            ? `${topic.avg_duration}ms`
                            : "--"}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-3">
                      <Link
                        to="/topics/$internalName"
                        params={{ internalName: topic.internal_name }}
                        className="px-6 py-2 bg-kb-bg border border-kb-border text-[9px] font-black uppercase tracking-[0.2em] hover:bg-kb-neon hover:text-black hover:border-kb-neon transition-all shadow-md group/btn relative overflow-hidden"
                      >
                        <span className="relative z-10">Manage</span>
                        <div className="absolute inset-0 bg-kb-neon translate-y-full group-hover/btn:translate-y-0 transition-transform duration-300" />
                      </Link>
                    </div>
                  </div>

                  {/* Dynamic Progress Line */}
                  <div className="h-[2px] w-full bg-kb-bg relative overflow-hidden">
                    <motion.div
                      initial={{ x: "-100%" }}
                      animate={{ x: "100%" }}
                      transition={{
                        duration: 5 + Math.random() * 5,
                        repeat: Infinity,
                        ease: "linear",
                      }}
                      className={`absolute inset-0 bg-gradient-to-r from-transparent via-kb-neon/40 to-transparent w-1/3 ${topic.paused ? "via-kb-warning/20" : ""}`}
                    />
                  </div>
                </motion.div>
              ))}
              {filteredTopics.length === 0 && (
                <div className="py-20 text-center text-kb-subtext uppercase text-[12px] font-black tracking-[0.5em] italic opacity-30 border-2 border-dashed border-kb-border">
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
