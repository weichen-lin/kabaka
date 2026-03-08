import { create } from "zustand";
import { persist } from "zustand/middleware";

interface UIState {
  theme: "dark" | "light";
  sidebarOpen: boolean;
  activeTopic: string | null;
  activeView: "dash" | "topics" | "workers" | "stats";
  wsStatus: "connected" | "disconnected" | "connecting";
  toggleTheme: () => void;
  toggleSidebar: () => void;
  setActiveTopic: (topic: string | null) => void;
  setActiveView: (view: "dash" | "topics" | "workers" | "stats") => void;
  setWSStatus: (status: "connected" | "disconnected" | "connecting") => void;
}

export const useStore = create<UIState>()(
  persist(
    (set) => ({
      theme: "dark",
      sidebarOpen: true,
      activeTopic: null,
      activeView: "dash",
      wsStatus: "disconnected",
      toggleTheme: () =>
        set((state) => ({ theme: state.theme === "dark" ? "light" : "dark" })),
      toggleSidebar: () =>
        set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setActiveTopic: (topic) => set({ activeTopic: topic }),
      setActiveView: (view) => set({ activeView: view }),
      setWSStatus: (status) => set({ wsStatus: status }),
    }),
    {
      name: "kabaka-ui-storage",
      partialize: (state) => ({
        theme: state.theme,
        sidebarOpen: state.sidebarOpen,
      }),
    },
  ),
);
