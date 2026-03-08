export const StatusTag = ({
  label,
  status,
}: {
  label: string;
  status: "ok" | "err" | "warn";
}) => {
  const styles = {
    ok: "text-kb-neon border-kb-neon/30 bg-kb-neon/5",
    err: "text-red-500 border-red-500/30 bg-red-500/5",
    warn: "text-kb-warning border-kb-warning/30 bg-kb-warning/5",
  };
  return (
    <div
      className={`text-[10px] font-bold px-2 py-0.5 border uppercase tracking-tighter flex items-center gap-1.5 ${styles[status]}`}
    >
      <span
        className={`w-1 h-1 rounded-full ${status === "ok" ? "animate-pulse bg-current" : "bg-current"}`}
      />
      {label}
    </div>
  );
};
