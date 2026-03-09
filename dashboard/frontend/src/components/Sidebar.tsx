import { Link } from "@tanstack/react-router";
import { motion } from "framer-motion";
import { Database, Terminal } from "lucide-react";
import { useStore } from "../store/useStore";

export const Sidebar = () => {
  const { sidebarOpen, theme } = useStore();

  const navItems = [
    { to: "/", icon: Terminal, label: "System Overview" },
    { to: "/topics", icon: Database, label: "Topic Registry" },
  ] as const;

  return (
    <motion.aside
      initial={false}
      animate={{ width: sidebarOpen ? 240 : 64 }}
      className="border-r border-kb-border bg-kb-card/80 backdrop-blur-md flex flex-col relative z-20"
    >
      <div
        className={`h-16 border-b border-kb-border flex items-center shrink-0 overflow-hidden transition-all ${sidebarOpen ? "px-6" : "px-3 justify-center"}`}
      >
        <div className="flex items-center gap-4">
          <div className="w-10 h-10 flex items-center justify-center shrink-0">
            <img
              src={theme === "dark" ? "/icon_dark.svg" : "/icon.svg"}
              alt="Kabaka Logo"
              className="w-full h-full object-contain filter brightness-125"
            />
          </div>
          {sidebarOpen && (
            <motion.span
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              className="font-black italic text-lg tracking-tighter text-kb-text whitespace-nowrap uppercase"
            >
              Kabaka
            </motion.span>
          )}
        </div>
      </div>

      <nav className="flex-1 py-6 space-y-1 overflow-y-auto overflow-x-hidden">
        {navItems.map((item) => (
          <Link
            key={item.to}
            to={item.to}
            className={`w-full flex items-center ${sidebarOpen ? "px-6 gap-4" : "px-0 justify-center"} py-3.5 group transition-all text-left relative [&.active]:bg-kb-neon/5`}
          >
            {/* Active indicator using TanStack Router's active class */}
            <div className="absolute left-0 w-1 h-6 bg-kb-neon opacity-0 group-[.active]:opacity-100 transition-opacity" />

            <item.icon
              size={18}
              className={`shrink-0 transition-colors text-kb-subtext group-[.active]:text-kb-neon group-hover:text-kb-neon`}
            />
            {sidebarOpen && (
              <span
                className={`text-[11px] uppercase font-black tracking-widest transition-colors whitespace-nowrap text-kb-subtext group-[.active]:text-kb-text group-hover:text-kb-text`}
              >
                {item.label}
              </span>
            )}
          </Link>
        ))}
      </nav>

      <div
        className={`p-5 border-t border-kb-border text-[9px] text-kb-subtext uppercase font-bold overflow-hidden transition-all ${!sidebarOpen && "text-center"}`}
      >
        {sidebarOpen ? "© 2026 WEI CHEN LIN" : "v1.0"}
      </div>
    </motion.aside>
  );
};
