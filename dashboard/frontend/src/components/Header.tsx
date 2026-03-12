import { Menu, Moon, Sun, Wifi, WifiOff } from "lucide-react";
import { useStore } from "../store/useStore";

export const Header = () => {
  const { theme, toggleTheme, toggleSidebar, wsStatus } = useStore();

  return (
    <header className="h-16 border-b border-kb-border px-8 flex justify-between items-center bg-kb-card/50 backdrop-blur-sm relative z-20">
      <div className="flex items-center gap-6">
        <button
          type="button"
          onClick={toggleSidebar}
          className="text-kb-subtext hover:text-kb-text transition-all hover:scale-110 active:scale-95"
        >
          <Menu size={18} />
        </button>

        <div className="h-4 w-[1px] bg-kb-border mx-2" />

        <div className="flex items-center gap-2">
          <div
            className={`w-2 h-2 rounded-full ${
              wsStatus === "connected"
                ? "bg-kb-neon shadow-[0_0_8px_var(--kb-neon)]"
                : wsStatus === "connecting"
                  ? "bg-kb-warning animate-pulse"
                  : "bg-red-500"
            }`}
          />
          <span className="text-[9px] font-black uppercase tracking-widest text-kb-subtext">
            {wsStatus === "connected" ? (
              <span className="flex items-center gap-1">
                <Wifi size={10} /> Live Sync
              </span>
            ) : (
              <span className="flex items-center gap-1">
                <WifiOff size={10} /> {wsStatus.toUpperCase()}
              </span>
            )}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-6">
        <button
          type="button"
          onClick={toggleTheme}
          className="p-2 border border-kb-border bg-kb-card hover:border-kb-neon transition-all group overflow-hidden relative"
        >
          {theme === "dark" ? (
            <Sun
              size={16}
              className="text-kb-subtext group-hover:text-kb-neon relative z-10"
            />
          ) : (
            <Moon
              size={16}
              className="text-kb-subtext group-hover:text-kb-neon relative z-10"
            />
          )}
          <div className="absolute inset-0 bg-kb-neon/5 translate-y-full group-hover:translate-y-0 transition-transform" />
        </button>
      </div>
    </header>
  );
};
