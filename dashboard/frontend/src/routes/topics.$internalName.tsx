import { createFileRoute, Link } from "@tanstack/react-router";
import JsonView from "@uiw/react-json-view";
import { lightTheme } from "@uiw/react-json-view/light";
import { vscodeTheme } from "@uiw/react-json-view/vscode";
import { AnimatePresence, motion } from "framer-motion";
import {
  Activity,
  ArrowLeft,
  Clock,
  Code,
  History as HistoryIcon,
  Layers,
  Loader2,
  Pause,
  Play,
  RotateCcw,
  Send,
  Settings,
  ShieldCheck,
  Trash2,
  X,
} from "lucide-react";
import { useEffect, useState } from "react";
import { toast } from "sonner";
import {
  useTopicActions,
  useTopicDetail,
  useTopicHistory,
  useTopics,
} from "../api/queries";
import { SchemaForm } from "../components/SchemaForm";
import { StatusTag } from "../components/StatusTag";
import { useStore } from "../store/useStore";

export const Route = createFileRoute("/topics/$internalName")({
  component: TopicDetail,
});

interface HistoryRecord {
  id: string;
  topic: string;
  payload: unknown;
  status: "success" | "dead";
  error?: string;
  attempts: number;
  duration_ms: number;
  created_at: string;
  finished_at: string;
}

function TopicDetail() {
  const { internalName } = Route.useParams();
  const { openConfirm, theme } = useStore();
  const [activeTab, setActiveTab] = useState<"schema" | "publish" | "history">(
    "publish",
  );
  const [currentPayload, setCurrentPayload] = useState<unknown>(null);
  const [selectedHistory, setSelectedHistory] = useState<HistoryRecord | null>(
    null,
  );

  // 1. 先拿所有 Topics 列表來做「反向查找」
  const { data: topicsData, isLoading: isLoadingList } = useTopics();

  // 2. 找到對應的 topic name (後端 API 仍需要使用 name 作為 Key)
  const targetTopicInfo = topicsData?.topics.find(
    (t) => t.internal_name === internalName,
  );

  // 3. 拿到真實的 name 後，去拿詳細資料 (Schema 等)
  const topicName = targetTopicInfo?.name || null;
  const {
    data: topic,
    isLoading: isLoadingDetail,
    error,
  } = useTopicDetail(topicName);

  const { data: historyData, isLoading: isLoadingHistory } =
    useTopicHistory(topicName);

  const {
    pause,
    resume,
    purge,
    publish,
    isPausing,
    isResuming,
    isPurging,
    isPublishing,
  } = useTopicActions();

  const isLoading = isLoadingList || (topicName && isLoadingDetail);

  if (isLoading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center min-h-[60vh] gap-6 relative z-10">
        <div className="relative">
          <div className="w-16 h-16 border-2 border-kb-neon/20 border-t-kb-neon rounded-full animate-spin" />
          <div className="absolute inset-0 w-16 h-16 border-2 border-transparent border-b-kb-info/40 rounded-full animate-spin-reverse" />
        </div>
        <div className="space-y-1 text-center">
          <p className="text-kb-neon font-black uppercase tracking-[0.5em] text-[10px] animate-pulse">
            Resolving Registry...
          </p>
          <p className="text-kb-subtext font-bold uppercase tracking-widest text-[8px] opacity-50">
            Mapping {internalName} to registry record
          </p>
        </div>
      </div>
    );
  }

  if (error || !topic) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center min-h-[60vh] p-8 text-center relative z-10">
        <div className="w-20 h-20 bg-red-500/10 border border-red-500/30 flex items-center justify-center mb-6 rotate-45">
          <div className="-rotate-45">
            <Trash2 size={32} className="text-red-500" />
          </div>
        </div>
        <h2 className="text-4xl font-black text-red-500 uppercase italic tracking-tighter mb-2">
          404: Topic Not Found
        </h2>
        <p className="text-kb-subtext font-bold uppercase tracking-widest text-[10px] mb-8">
          The requested topic internal identifier ({internalName}) does not
          exist
        </p>
        <Link
          to="/topics"
          className="px-8 py-3 bg-kb-neon text-black font-black uppercase italic text-xs hover:brightness-110 transition-all flex items-center gap-3"
        >
          <ArrowLeft size={16} /> Return to Registry
        </Link>
      </div>
    );
  }

  const handlePauseToggle = () => {
    if (!topicName) return;
    const isPaused = topic.paused;

    if (isPaused) {
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
    if (!topicName) return;
    openConfirm({
      title: "Purge Topic Queue",
      description: `Target Topic: ${topicName}`,
      message: (
        <>
          This action will{" "}
          <span className="font-bold text-kb-text">PERMANENTLY DELETE</span> all
          pending and delayed tasks for internal queue{" "}
          <code className="text-[10px] bg-kb-bg px-1">{internalName}</code>.
        </>
      ),
      variant: "danger",
      onConfirm: () =>
        purge(topicName, {
          onSuccess: () => toast.success(`Queue for "${topicName}" purged`),
          onError: (err) => toast.error(`Purge failed: ${err.message}`),
        }),
    });
  };

  const handlePublish = async (data: unknown) => {
    if (!topicName) return;
    const promise = publish({ name: topicName, data });
    toast.promise(promise, {
      loading: "Publishing message...",
      success: "Message published successfully",
      error: (err) => `Publish failed: ${err.message}`,
    });
  };

  return (
    <div className="h-full flex flex-col p-8 gap-4 relative z-10 overflow-hidden">
      {/* Breadcrumbs & Header - Shrink-0 to keep fixed height */}
      <header className="flex flex-col md:flex-row md:items-end justify-between gap-6 shrink-0">
        <div className="space-y-4">
          <Link
            to="/topics"
            className="inline-flex items-center gap-2 text-kb-subtext hover:text-kb-neon transition-colors uppercase text-[10px] font-black tracking-widest"
          >
            <ArrowLeft size={14} /> Back to Topics
          </Link>
          <div className="flex items-center gap-4">
            <div className="w-16 h-16 bg-kb-card border border-kb-neon/30 flex items-center justify-center">
              <Settings className="text-kb-neon" size={32} />
            </div>
            <div>
              <div className="flex items-center gap-3">
                <div className="flex flex-col space-y-1">
                  <div className="flex gap-x-4 items-center">
                    <h1 className="text-4xl font-black italic tracking-tighter uppercase leading-none">
                      {topicName}
                    </h1>
                    <StatusTag
                      label={topic.paused ? "Paused" : "Active"}
                      status={topic.paused ? "paused" : "ok"}
                    />
                  </div>
                  <span className="text-[11px] text-kb-neon font-mono font-bold tracking-tight">
                    {internalName}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="flex gap-3">
          <button
            type="button"
            onClick={handlePauseToggle}
            disabled={isPausing || isResuming}
            className={`flex items-center gap-2 px-6 py-3 font-black text-xs uppercase italic transition-all min-w-[160px] justify-center ${
              topic.paused
                ? "bg-kb-text text-slate-300 hover:bg-kb-text/90"
                : "bg-kb-neon text-black hover:brightness-110"
            } disabled:opacity-50 disabled:cursor-not-allowed`}
          >
            {isPausing || isResuming ? (
              <Loader2 size={16} className="animate-spin" />
            ) : topic.paused ? (
              <Play size={16} fill="currentColor" />
            ) : (
              <Pause size={16} />
            )}
            {isPausing
              ? "Pausing..."
              : isResuming
                ? "Resuming..."
                : topic.paused
                  ? "Resume Topic"
                  : "Pause Topic"}
          </button>
          <button
            type="button"
            onClick={handlePurge}
            disabled={isPurging}
            className="flex items-center gap-2 px-6 py-3 border border-red-500/30 text-red-500 font-black text-xs uppercase hover:bg-red-500/10 transition-colors disabled:opacity-50 disabled:cursor-not-allowed min-w-[140px] justify-center"
          >
            {isPurging ? (
              <Loader2 size={16} className="animate-spin" />
            ) : (
              <Trash2 size={16} />
            )}
            {isPurging ? "Purging..." : "Purge Queue"}
          </button>
        </div>
      </header>

      {/* Main Grid - flex-1 and min-h-0 to allow scrolling inside children */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 items-stretch flex-1 min-h-0">
        {/* Left Column: Stats & Config */}
        <div className="lg:col-span-1 flex flex-col gap-4">
          {/* Quick Stats */}
          <section className="bg-kb-card border border-kb-border p-6 space-y-6">
            <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
              <Activity size={12} /> Performance
            </h3>
            <div className="grid grid-cols-2 gap-4">
              {[
                {
                  label: "Processed",
                  value: topic.processed_total,
                  color: "text-kb-text",
                },
                {
                  label: "Failed",
                  value: topic.failed_total,
                  color: "text-red-500",
                },
                {
                  label: "Avg Time",
                  value:
                    topic.avg_duration !== null
                      ? `${topic.avg_duration}ms`
                      : "--",
                  color: "text-kb-info",
                },
                {
                  label: "Success",
                  value:
                    topic.success_rate !== null
                      ? `${topic.success_rate}%`
                      : "--",
                  color: "text-kb-neon",
                },
              ].map((s) => (
                <div key={s.label} className="space-y-1">
                  <p className="text-[9px] font-black text-kb-subtext uppercase">
                    {s.label}
                  </p>
                  <p className={`text-xl font-black ${s.color}`}>
                    {s.value.toLocaleString()}
                  </p>
                </div>
              ))}
            </div>
          </section>

          {/* Configuration */}
          <section className="bg-kb-card border border-kb-border p-6 space-y-6">
            <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
              <Settings size={12} /> Config
            </h3>
            <div className="space-y-4">
              {[
                {
                  label: "Max Retries",
                  value: `${topic.max_retries}`,
                  icon: <RotateCcw size={14} />,
                },
                {
                  label: "Retry Delay",
                  value: `${topic.retry_delay}s`,
                  icon: <Clock size={14} />,
                },
                {
                  label: "Timeout",
                  value: `${topic.process_timeout}s`,
                  icon: <ShieldCheck size={14} />,
                },
                {
                  label: "History",
                  value:
                    topic.history_limit > 0 ? `${topic.history_limit}` : "Off",
                  icon: <HistoryIcon size={14} />,
                },
              ].map((i) => (
                <div
                  key={i.label}
                  className="flex items-center justify-between text-xs"
                >
                  <div className="flex items-center gap-2 text-kb-subtext uppercase font-black tracking-widest text-[9px]">
                    {i.icon} {i.label}
                  </div>
                  <span className="font-bold">{i.value}</span>
                </div>
              ))}
            </div>
          </section>

          {/* Queue Status */}
          <section className="bg-kb-card border border-kb-border p-6 space-y-6 flex-1">
            <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
              <Layers size={12} /> Queue Status
            </h3>
            <div className="space-y-3">
              {[
                {
                  label: "Pending",
                  value: topic.queue_pending,
                  color: "bg-kb-neon",
                },
                {
                  label: "Delayed",
                  value: topic.queue_delayed,
                  color: "bg-kb-warning",
                },
                {
                  label: "Processing",
                  value: topic.queue_processing,
                  color: "bg-kb-info",
                },
              ].map((item) => (
                <div
                  key={item.label}
                  className="flex justify-between items-center p-3 bg-kb-bg/50 border border-kb-border"
                >
                  <span className="text-[9px] font-black uppercase tracking-widest text-kb-subtext">
                    {item.label}
                  </span>
                  <div className="flex items-center gap-2">
                    <div
                      className={`h-1.5 w-1.5 rounded-full ${item.color} animate-pulse`}
                    />
                    <span className="text-sm font-black">{item.value}</span>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>

        {/* Right Column: Schema & Publish & History */}
        <div className="lg:col-span-2 min-h-0">
          <section className="bg-kb-card border border-kb-border h-full flex flex-col overflow-hidden relative">
            {/* Cyber Tab Header */}
            <div className="flex items-center justify-between border-b border-kb-border shrink-0 bg-kb-bg/40">
              <div className="flex">
                <button
                  type="button"
                  onClick={() => setActiveTab("publish")}
                  className={`group relative px-8 py-4 text-[11px] font-black uppercase italic tracking-[0.2em] transition-all flex items-center gap-3 border-r border-kb-border ${
                    activeTab === "publish"
                      ? "text-kb-neon"
                      : "text-kb-subtext hover:text-kb-text"
                  }`}
                >
                  <Send
                    size={14}
                    className={activeTab === "publish" ? "animate-pulse" : ""}
                  />
                  <span>Publish Message</span>
                  {activeTab === "publish" && (
                    <motion.div
                      layoutId="active_tab_glitch"
                      className="absolute bottom-0 left-0 right-0 h-[2px] bg-kb-neon shadow-[0_0_10px_rgba(0,255,159,0.5)]"
                    />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => setActiveTab("schema")}
                  className={`group relative px-8 py-4 text-[11px] font-black uppercase italic tracking-[0.2em] transition-all flex items-center gap-3 border-r border-kb-border ${
                    activeTab === "schema"
                      ? "text-kb-neon"
                      : "text-kb-subtext hover:text-kb-text"
                  }`}
                >
                  <Code size={14} />
                  <span>Topic Schema</span>
                  {activeTab === "schema" && (
                    <motion.div
                      layoutId="active_tab_glitch"
                      className="absolute bottom-0 left-0 right-0 h-[2px] bg-kb-neon shadow-[0_0_10px_rgba(0,255,159,0.5)]"
                    />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => setActiveTab("history")}
                  className={`group relative px-8 py-4 text-[11px] font-black uppercase italic tracking-[0.2em] transition-all flex items-center gap-3 border-r border-kb-border ${
                    activeTab === "history"
                      ? "text-kb-neon"
                      : "text-kb-subtext hover:text-kb-text"
                  }`}
                >
                  <HistoryIcon size={14} />
                  <span>Audit History</span>
                  {activeTab === "history" && (
                    <motion.div
                      layoutId="active_tab_glitch"
                      className="absolute bottom-0 left-0 right-0 h-[2px] bg-kb-neon shadow-[0_0_10px_rgba(0,255,159,0.5)]"
                    />
                  )}
                </button>
              </div>

              <div className="px-6 flex items-center gap-4">
                {topic.schema ? (
                  <div className="flex items-center gap-2">
                    <div className="w-1.5 h-1.5 bg-kb-neon rounded-full animate-pulse shadow-[0_0_5px_rgba(0,255,159,0.5)]" />
                    <span className="text-[9px] font-black text-kb-neon uppercase tracking-widest">
                      Schema Ready
                    </span>
                  </div>
                ) : (
                  <div className="flex items-center gap-2">
                    <div className="w-1.5 h-1.5 bg-kb-subtext rounded-full" />
                    <span className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                      Schemaless Mode
                    </span>
                  </div>
                )}
              </div>
            </div>

            <div className="flex-1 overflow-hidden relative">
              <AnimatePresence mode="wait">
                {activeTab === "publish" ? (
                  <motion.div
                    key="publish"
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                    className="h-full flex flex-col md:flex-row divide-x divide-kb-border"
                  >
                    {/* Input Area */}
                    <div className="flex-1 min-h-0 flex flex-col p-6 bg-kb-bg/40 relative">
                      {topic.schema ? (
                        <SchemaForm
                          schema={topic.schema}
                          onSubmit={handlePublish}
                          onChange={setCurrentPayload}
                          isLoading={isPublishing}
                        />
                      ) : (
                        <div className="h-full flex flex-col items-center justify-center text-center p-8 space-y-4 opacity-50">
                          <Code size={32} className="text-kb-subtext" />
                          <p className="text-[10px] font-black uppercase tracking-widest">
                            Schemaless Publishing
                          </p>
                        </div>
                      )}
                    </div>

                    {/* Preview Area */}
                    <div className="flex-1 min-h-0 flex flex-col bg-kb-bg/60 relative group/preview">
                      {!!currentPayload && (
                        <div className="absolute top-4 right-6 z-20 opacity-0 group-hover/preview:opacity-100 transition-opacity">
                          <button
                            type="button"
                            onClick={() => {
                              navigator.clipboard.writeText(
                                JSON.stringify(currentPayload, null, 2),
                              );
                              toast.success("Payload copied to clipboard");
                            }}
                            className="px-3 py-1.5 bg-kb-bg border border-kb-border text-[9px] font-black uppercase text-kb-neon hover:border-kb-neon transition-colors shadow-xl"
                          >
                            Copy JSON
                          </button>
                        </div>
                      )}

                      <div className="flex-1 overflow-hidden p-6 relative">
                        <div className="absolute inset-6 overflow-y-auto custom-scrollbar">
                          <JsonView
                            value={currentPayload || {}}
                            style={{
                              ...(theme === "dark" ? vscodeTheme : lightTheme),
                              backgroundColor: "transparent",
                            }}
                            displayDataTypes={false}
                            displayObjectSize={true}
                            enableClipboard={false}
                            shortenTextAfterLength={100}
                            indentWidth={24}
                          />
                          {!currentPayload && (
                            <div className="h-full flex items-center justify-center opacity-20 pointer-events-none">
                              <p className="text-[10px] font-black uppercase tracking-[0.8em] animate-pulse">
                                Waiting for data...
                              </p>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  </motion.div>
                ) : activeTab === "schema" ? (
                  <motion.div
                    key="schema"
                    initial={{ opacity: 0, scale: 0.98 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 1.02 }}
                    className="h-full flex flex-col"
                  >
                    <div className="flex-1 p-6 overflow-hidden bg-kb-bg/40 relative">
                      <div className="absolute inset-6 overflow-y-auto custom-scrollbar">
                        <pre className="text-[11px] font-mono text-kb-text leading-relaxed">
                          {(() => {
                            if (!topic.schema) return "No schema defined";
                            try {
                              return JSON.stringify(
                                JSON.parse(topic.schema),
                                null,
                                2,
                              );
                            } catch {
                              return "Invalid schema format";
                            }
                          })()}
                        </pre>
                      </div>
                    </div>
                  </motion.div>
                ) : (
                  <motion.div
                    key="history"
                    initial={{ opacity: 0, x: 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                    className="h-full overflow-y-auto custom-scrollbar p-6 space-y-1.5"
                  >
                    {topic.history_limit === 0 ? (
                      <div className="h-full flex flex-col items-center justify-center text-center p-8">
                        <div className="relative mb-6">
                          <div className="w-20 h-20 border border-kb-border flex items-center justify-center bg-kb-bg/40 rotate-3">
                            <HistoryIcon
                              size={28}
                              className="text-kb-subtext/30"
                            />
                          </div>
                          <div className="absolute -top-1 -right-1 w-5 h-5 bg-kb-bg border border-kb-border flex items-center justify-center">
                            <X size={10} className="text-kb-subtext/50" />
                          </div>
                        </div>
                        <p className="text-xs font-black uppercase tracking-[0.3em] text-kb-subtext/60 mb-2">
                          Audit Disabled
                        </p>
                        <p className="text-[10px] text-kb-subtext/40 max-w-[240px] leading-relaxed">
                          Set{" "}
                          <span className="font-black text-kb-subtext/60">
                            HistoryLimit
                          </span>{" "}
                          to enable execution trace recording for this topic.
                        </p>
                      </div>
                    ) : isLoadingHistory ? (
                      <div className="h-full flex flex-col items-center justify-center gap-4 opacity-40">
                        <Loader2 className="animate-spin text-kb-neon" />
                        <span className="text-[10px] font-black uppercase tracking-widest">
                          Syncing Audit Log...
                        </span>
                      </div>
                    ) : !historyData || historyData.history.length === 0 ? (
                      <div className="h-full flex flex-col items-center justify-center text-center opacity-30 gap-4">
                        <HistoryIcon size={48} />
                        <div>
                          <p className="text-xs font-black uppercase tracking-widest mb-1">
                            No History Records
                          </p>
                          <p className="text-[10px] uppercase tracking-tighter">
                            Waiting for the first execution trace
                          </p>
                        </div>
                      </div>
                    ) : (
                      historyData.history.map((record: HistoryRecord) => (
                        <HistoryItem
                          key={record.id}
                          record={record}
                          onView={() => setSelectedHistory(record)}
                        />
                      ))
                    )}
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </section>
        </div>
      </div>

      {/* Audit Detail Popup */}
      <AnimatePresence>
        {selectedHistory && (
          <HistoryDetailPopup
            record={selectedHistory}
            onClose={() => setSelectedHistory(null)}
            theme={theme}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

function HistoryItem({
  record,
  onView,
}: {
  record: HistoryRecord;
  onView: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onView}
      className="w-full bg-kb-bg/40 border border-kb-border/50 py-2 px-4 flex items-center justify-between group transition-all hover:border-kb-neon/40 hover:bg-kb-neon/5 select-none outline-none"
    >
      <div className="flex items-center gap-3 text-left">
        <div
          className={`w-1.5 h-1.5 rounded-full ${
            record.status === "success"
              ? "bg-kb-neon shadow-[0_0_8px_rgba(0,255,159,0.5)]"
              : "bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.5)]"
          }`}
        />
        <div className="flex items-center gap-3">
          <span className="text-[9px] font-mono text-kb-subtext font-bold opacity-60">
            #{record.id.slice(0, 8)}
          </span>
          <span
            className={`text-[10px] font-black uppercase italic tracking-tighter ${
              record.status === "success" ? "text-kb-neon" : "text-red-500"
            }`}
          >
            {record.status}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-6">
        <div className="hidden md:flex flex-col items-end">
          <span className="text-[10px] font-mono font-bold text-kb-info/70">
            {record.duration_ms}ms
          </span>
        </div>
        <div className="flex flex-col items-end min-w-[100px]">
          <span className="text-[9px] font-mono font-bold text-kb-subtext opacity-50">
            {new Date(record.finished_at).toLocaleTimeString([], {
              hour12: false,
            })}
          </span>
        </div>
      </div>
    </button>
  );
}

function HistoryDetailPopup({
  record,
  onClose,
  theme,
}: {
  record: HistoryRecord;
  onClose: () => void;
  theme: string;
}) {
  const displayPayload = record.payload ?? {};

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-[100] flex items-center justify-center p-4 md:p-8"
    >
      {/* Backdrop */}
      <button
        type="button"
        className="absolute inset-0 bg-kb-bg/80 backdrop-blur-md border-none p-0 cursor-default"
        onClick={onClose}
        tabIndex={-1}
        onKeyDown={(e) => e.key === "Escape" && onClose()}
      />

      <motion.div
        initial={{ scale: 0.95, opacity: 0, y: 20 }}
        animate={{ scale: 1, opacity: 1, y: 0 }}
        exit={{ scale: 0.95, opacity: 0, y: 20 }}
        className="relative w-full max-w-2xl max-h-[80vh] bg-kb-card border border-kb-border shadow-2xl flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="p-6 border-b border-kb-border bg-kb-bg/40 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div
              className={`w-2 h-2 rounded-full ${
                record.status === "success" ? "bg-kb-neon" : "bg-red-500"
              }`}
            />
            <div>
              <h2 className="text-xl font-black italic tracking-tighter uppercase leading-none">
                Audit Record Detail
              </h2>
              <p className="text-[10px] font-mono font-bold text-kb-subtext mt-1 opacity-60">
                ID: {record.id}
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="p-2 text-kb-subtext hover:text-kb-neon transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 custom-scrollbar space-y-6">
          <div className="grid grid-cols-3 gap-4">
            {[
              {
                label: "Status",
                value: record.status,
                color:
                  record.status === "success" ? "text-kb-neon" : "text-red-500",
              },
              {
                label: "Duration",
                value: `${record.duration_ms}ms`,
                color: "text-kb-info",
              },
              {
                label: "Attempts",
                value: `${record.attempts}x`,
                color: "text-kb-text",
              },
            ].map((s) => (
              <div
                key={s.label}
                className="bg-kb-bg/30 border border-kb-border/50 p-3"
              >
                <p className="text-[8px] font-black uppercase text-kb-subtext tracking-widest mb-1">
                  {s.label}
                </p>
                <p className={`text-sm font-black italic uppercase ${s.color}`}>
                  {s.value}
                </p>
              </div>
            ))}
          </div>

          {record.error && (
            <div className="p-4 bg-red-500/10 border-l-2 border-red-500 space-y-1">
              <p className="text-[9px] font-black text-red-500 uppercase tracking-widest">
                Execution Error
              </p>
              <p className="text-xs font-mono text-red-400 leading-relaxed">
                {record.error}
              </p>
            </div>
          )}

          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                Payload Trace
              </span>
              <button
                type="button"
                onClick={() => {
                  navigator.clipboard.writeText(
                    JSON.stringify(displayPayload, null, 2),
                  );
                  toast.success("Payload copied");
                }}
                className="text-[9px] font-black text-kb-neon uppercase hover:underline"
              >
                Copy JSON
              </button>
            </div>
            <div className="bg-kb-bg/60 p-4 border border-kb-border/50 font-mono">
              <JsonView
                value={displayPayload}
                style={{
                  ...(theme === "dark" ? vscodeTheme : lightTheme),
                  backgroundColor: "transparent",
                  fontSize: "12px",
                }}
                displayDataTypes={false}
                displayObjectSize={true}
                enableClipboard={false}
                indentWidth={24}
              />
            </div>
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
}
