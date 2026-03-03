import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface UIState {
  theme: 'dark' | 'light';
  sidebarOpen: boolean;
  activeTopic: string | null;
  toggleTheme: () => void;
  toggleSidebar: () => void;
  setActiveTopic: (topic: string | null) => void;
}

export const useStore = create<UIState>()(
  persist(
    (set) => ({
      theme: 'dark',
      sidebarOpen: true,
      activeTopic: null,
      toggleTheme: () => set((state) => ({ theme: state.theme === 'dark' ? 'light' : 'dark' })),
      toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setActiveTopic: (topic) => set({ activeTopic: topic }),
    }),
    { name: 'kabaka-ui-storage' }
  )
);
