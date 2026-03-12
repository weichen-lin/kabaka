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
  FileJson,
  Loader2,
  Pause,
  Play,
  RotateCcw,
  Send,
  Settings,
  ShieldCheck,
  Trash2,
} from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import { useTopicActions, useTopicDetail, useTopics } from "../api/queries";
import { SchemaForm } from "../components/SchemaForm";
import { StatusTag } from "../components/StatusTag";
import { useStore } from "../store/useStore";

export const Route = createFileRoute("/topics/$internalName")({
  component: TopicDetail,
});

function TopicDetail() {
  const { internalName } = Route.useParams();
  const { openConfirm, theme } = useStore();
  const [activeTab, setActiveTab] = useState<"schema" | "publish">("publish");
  const [currentPayload, setCurrentPayload] = useState<unknown>(null);

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
            Resolving_Internal_Registry...
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
          404: Topic_Not_Found
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
      title: "Purge_Topic_Queue",
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
    <div className="h-full flex flex-col p-8 gap-8 relative z-10 overflow-hidden">
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
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 items-stretch flex-1 min-h-0">
        {/* Left Column: Stats & Config */}
        <div className="lg:col-span-1 flex flex-col gap-6 overflow-y-auto custom-scrollbar pr-2">
          {/* Quick Stats */}
          <section className="bg-kb-card border border-kb-border p-6 space-y-6 shrink-0">
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
                  value: `${topic.avg_duration}ms`,
                  color: "text-kb-info",
                },
                {
                  label: "Success",
                  value: `${topic.success_rate}%`,
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
          <section className="bg-kb-card border border-kb-border p-6 space-y-6 shrink-0">
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
          <section className="bg-kb-card border border-kb-border p-6 space-y-6 shrink-0">
            <h3 className="text-[10px] font-black uppercase tracking-[0.3em] text-kb-subtext">
              Queue Status
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

        {/* Right Column: Schema & Publish */}
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
                  <span>Publish_Message</span>
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
                  <span>Actual_Schema</span>
                  {activeTab === "schema" && (
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
                      Schema_Valid
                    </span>
                  </div>
                ) : (
                  <div className="flex items-center gap-2">
                    <div className="w-1.5 h-1.5 bg-kb-subtext rounded-full" />
                    <span className="text-[9px] font-black text-kb-subtext uppercase tracking-widest">
                      No_Schema
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
                    <div className="flex-1 min-h-0 flex flex-col">
                      <div className="p-4 border-b border-kb-border bg-kb-bg/20 flex items-center justify-between">
                        <h4 className="text-[9px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
                          <Settings size={12} className="text-kb-neon" />{" "}
                          Input_Designer
                        </h4>
                      </div>
                      <div className="flex-1 overflow-y-auto custom-scrollbar p-6 bg-kb-bg/40">
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
                              Schema_Less_Mode_Active
                            </p>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Preview Area */}
                    <div className="flex-1 min-h-0 flex flex-col bg-kb-bg/60">
                      <div className="p-4 border-b border-kb-border bg-kb-bg/20 flex items-center justify-between">
                        <h4 className="text-[9px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
                          <FileJson size={12} className="text-kb-neon" />{" "}
                          Payload_Inspector
                        </h4>
                        {currentPayload && (
                          <button
                            type="button"
                            onClick={() => {
                              navigator.clipboard.writeText(
                                JSON.stringify(currentPayload, null, 2),
                              );
                              toast.success("Payload copied to clipboard");
                            }}
                            className="text-[9px] font-black uppercase text-kb-neon hover:underline"
                          >
                            Copy_JSON
                          </button>
                        )}
                      </div>
                      <div className="flex-1 overflow-hidden p-4 relative">
                        <div className="absolute inset-4 overflow-y-auto custom-scrollbar">
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
                                Waiting_For_Data...
                              </p>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  </motion.div>
                ) : (
                  <motion.div
                    key="schema"
                    initial={{ opacity: 0, scale: 0.98 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 1.02 }}
                    className="h-full flex flex-col"
                  >
                    <div className="p-4 border-b border-kb-border bg-kb-bg/20">
                      <h4 className="text-[9px] font-black uppercase tracking-[0.3em] text-kb-subtext flex items-center gap-2">
                        <Code size={12} className="text-kb-neon" />{" "}
                        JSON_Schema_Source
                      </h4>
                    </div>
                    <div className="flex-1 p-6 overflow-hidden bg-kb-bg/40 relative">
                      <div className="absolute inset-6 overflow-y-auto custom-scrollbar">
                        <pre className="text-[11px] font-mono text-kb-text leading-relaxed">
                          {topic.schema
                            ? JSON.stringify(JSON.parse(topic.schema), null, 2)
                            : "NO_SCHEMA_DEFINED"}
                        </pre>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
