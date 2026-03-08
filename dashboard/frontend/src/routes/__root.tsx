import { createRootRoute, Outlet } from "@tanstack/react-router";
import { useEffect } from "react";
import { useWebSocket } from "../api/queries";
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
          <div className="flex-1 overflow-y-auto">
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  );
}
