import { createRootRoute, Outlet } from "@tanstack/react-router";
import { useEffect } from "react";
import { Toaster } from "sonner";
import { useWebSocket } from "../api/queries";
import { ConfirmModal } from "../components/ConfirmModal";
import { Header } from "../components/Header";
import { Sidebar } from "../components/Sidebar";
import { useStore } from "../store/useStore";

export const Route = createRootRoute({
  component: RootLayout,
});

function RootLayout() {
  const { theme } = useStore();

  // Initialize WebSocket listener at the root level
  useWebSocket();

  useEffect(() => {
    const root = window.document.documentElement;
    if (theme === "light") {
      root.classList.add("light");
      root.classList.remove("dark");
    } else {
      root.classList.add("dark");
      root.classList.remove("light");
    }
  }, [theme]);

  return (
    <div className="min-h-screen bg-kb-bg text-kb-text relative overflow-hidden selection:bg-kb-neon selection:text-black transition-colors duration-300">
      {/* Visual Effects Background */}
      <div className="fixed inset-0 cyber-grid pointer-events-none" />
      <div className="fixed inset-0 scanline animate-scanline pointer-events-none" />
      <div className="fixed inset-0 noise-texture pointer-events-none" />

      <div className="relative z-10 flex h-screen overflow-hidden">
        <Sidebar />

        <main className="flex-1 flex flex-col overflow-hidden">
          <Header />
          <div className="flex-1 flex flex-col overflow-hidden relative">
            <Outlet />
          </div>
        </main>
      </div>

      <ConfirmModal />
      <Toaster
        position="top-right"
        theme={theme}
        icons={{
          success: null,
          info: null,
          warning: null,
          error: null,
          loading: (
            <div className="w-4 h-4 border-2 border-kb-neon border-t-transparent animate-spin mr-2" />
          ),
        }}
        toastOptions={{
          unstyled: true,
          classNames: {
            toast:
              "group w-[350px] flex flex-col justify-center bg-kb-card border border-kb-border p-4 shadow-2xl transition-all duration-300 pointer-events-auto select-none",
            title:
              "text-[11px] font-black uppercase tracking-[0.2em] font-mono leading-tight",
            description:
              "text-[10px] text-kb-subtext font-mono mt-1 leading-relaxed",
            success: "border-l-4 border-l-kb-neon",
            error: "border-l-4 border-l-destructive",
            warning: "border-l-4 border-l-kb-warning",
            info: "border-l-4 border-l-kb-info",
          },
        }}
      />
    </div>
  );
}
