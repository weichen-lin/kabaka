import { create } from "zustand";
import { persist } from "zustand/middleware";

interface UIState {
  theme: "dark" | "light";
  sidebarOpen: boolean;
  activeTopic: string | null;
  activeView: "dash" | "topics" | "workers" | "stats";
  toggleTheme: () => void;
  toggleSidebar: () => void;
  setActiveTopic: (topic: string | null) => void;
  setActiveView: (view: "dash" | "topics" | "workers" | "stats") => void;
}

export const useStore = create<UIState>()(
  persist(
    (set) => ({
      theme: "dark",
      sidebarOpen: true,
      activeTopic: null,
      activeView: "dash",
      toggleTheme: () =>
        set((state) => ({ theme: state.theme === "dark" ? "light" : "dark" })),
      toggleSidebar: () =>
        set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setActiveTopic: (topic) => set({ activeTopic: topic }),
      setActiveView: (view) => set({ activeView: view }),
    }),
    { name: "kabaka-ui-storage" },
  ),
);
