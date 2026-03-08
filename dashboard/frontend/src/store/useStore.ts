import { create } from "zustand";
import { persist } from "zustand/middleware";

interface ConfirmModalState {
  isOpen: boolean;
  title: string;
  description?: string;
  message: string | React.ReactNode;
  onConfirm: () => void;
  variant: "danger" | "warning";
}

interface UIState {
  theme: "dark" | "light";
  sidebarOpen: boolean;
  activeTopic: string | null;
  activeView: "dash" | "topics" | "workers" | "stats";
  wsStatus: "connected" | "disconnected" | "connecting";
  confirmModal: ConfirmModalState;
  toggleTheme: () => void;
  toggleSidebar: () => void;
  setActiveTopic: (topic: string | null) => void;
  setActiveView: (view: "dash" | "topics" | "workers" | "stats") => void;
  setWSStatus: (status: "connected" | "disconnected" | "connecting") => void;
  openConfirm: (opts: Omit<ConfirmModalState, "isOpen">) => void;
  closeConfirm: () => void;
}

export const useStore = create<UIState>()(
  persist(
    (set) => ({
      theme: "dark",
      sidebarOpen: true,
      activeTopic: null,
      activeView: "dash",
      wsStatus: "disconnected",
      confirmModal: {
        isOpen: false,
        title: "",
        message: "",
        variant: "warning",
        onConfirm: () => {},
      },
      toggleTheme: () =>
        set((state) => ({ theme: state.theme === "dark" ? "light" : "dark" })),
      toggleSidebar: () =>
        set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setActiveTopic: (topic) => set({ activeTopic: topic }),
      setActiveView: (view) => set({ activeView: view }),
      setWSStatus: (status) => set({ wsStatus: status }),
      openConfirm: (opts) => set({ confirmModal: { ...opts, isOpen: true } }),
      closeConfirm: () =>
        set((state) => ({
          confirmModal: { ...state.confirmModal, isOpen: false },
        })),
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
