import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/stats")({
  component: () => (
    <div className="flex-1 flex items-center justify-center p-8">
      <div className="text-kb-subtext uppercase font-black tracking-[0.5em] italic">
        Analyzing system metrics...
      </div>
    </div>
  ),
});
