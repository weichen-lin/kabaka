import React, { useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  Terminal, Database, Activity, Cpu, 
  ShieldAlert, ChevronRight, Server,
  Settings, Bell, Search, Info, Sun, Moon,
  Menu
} from 'lucide-react';
import { useStore } from './store/useStore';

const StatusTag = ({ label, status }: { label: string, status: 'ok' | 'err' | 'warn' }) => {
  const styles = {
    ok: 'text-kb-neon border-kb-neon/30 bg-kb-neon/5',
    err: 'text-red-500 border-red-500/30 bg-red-500/5',
    warn: 'text-kb-warning border-kb-warning/30 bg-kb-warning/5',
  };
  return (
    <div className={`text-[10px] font-bold px-2 py-0.5 border uppercase tracking-tighter flex items-center gap-1.5 ${styles[status]}`}>
      <span className={`w-1 h-1 rounded-full ${status === 'ok' ? 'animate-pulse bg-current' : 'bg-current'}`} />
      {label}
    </div>
  );
};

const StatCard = ({ label, val, unit, icon: Icon, color }: any) => (
  <motion.div 
    whileHover={{ scale: 1.02 }}
    className="bg-kb-card border border-kb-border p-5 relative group overflow-hidden"
  >
    <div className="flex justify-between items-start z-10 relative">
      <Icon size={14} className="text-kb-subtext group-hover:text-kb-text transition-colors" />
      <span className={`text-[10px] font-bold ${color}`}>+12.4%</span>
    </div>
    <div className="mt-4 flex items-baseline gap-1.5 z-10 relative">
      <span className="text-2xl font-black text-kb-text leading-none">{val}</span>
      <span className="text-[10px] text-kb-subtext uppercase tracking-tighter font-bold">{unit}</span>
    </div>
    <div className="mt-1 text-[10px] text-kb-subtext uppercase tracking-widest font-black z-10 relative">{label}</div>
    <Icon className="absolute -bottom-4 -right-4 text-kb-bg brightness-150 opacity-20 rotate-12" size={70} />
  </motion.div>
);

export default function App() {
  const { sidebarOpen, theme, activeTopic, toggleTheme, toggleSidebar, setActiveTopic } = useStore();

  useEffect(() => {
    const root = window.document.documentElement;
    if (theme === 'light') {
      root.classList.add('light');
    } else {
      root.classList.remove('light');
    }
  }, [theme]);

  return (
    <div className="min-h-screen bg-kb-bg text-kb-text relative overflow-hidden selection:bg-kb-neon selection:text-black transition-colors duration-300">
      <div className="fixed inset-0 cyber-grid pointer-events-none" />
      <div className="fixed inset-0 scanline animate-scanline pointer-events-none" />
      <div className="fixed inset-0 noise-texture pointer-events-none" />

      <div className="relative z-10 flex h-screen overflow-hidden">
        
        <motion.aside 
          initial={false}
          animate={{ width: sidebarOpen ? 240 : 64 }}
          className="border-r border-kb-border bg-kb-card/80 backdrop-blur-md flex flex-col"
        >
          <div className="h-16 border-b border-kb-border flex items-center px-6 shrink-0">
            <div className="flex items-center gap-4">
              <div className="w-8 h-8 flex items-center justify-center shrink-0">
                <img src="/logo.png" alt="Kabaka Logo" className="w-full h-full object-contain" />
              </div>
              {sidebarOpen && (
                <motion.span 
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  className="font-black italic text-base tracking-tighter text-kb-text whitespace-nowrap uppercase"
                >
                  KABAKA_OS
                </motion.span>
              )}
            </div>
          </div>

          <nav className="flex-1 py-6 space-y-1">
            {[
              { id: 'dash', icon: Terminal, label: 'Control_Center' },
              { id: 'topics', icon: Database, label: 'Topic_Registry' },
              { id: 'workers', icon: Server, label: 'Worker_Fleet' },
              { id: 'stats', icon: Activity, label: 'System_Metrics' },
            ].map(item => (
              <button 
                key={item.id}
                className="w-full flex items-center gap-4 px-6 py-3.5 hover:bg-kb-text/5 group transition-colors text-left"
              >
                <item.icon size={18} className="text-kb-subtext group-hover:text-kb-neon" />
                {sidebarOpen && <span className="text-[11px] uppercase font-black tracking-widest group-hover:text-kb-text">{item.label}</span>}
              </button>
            ))}
          </nav>

          <div className="p-5 border-t border-kb-border text-[9px] text-kb-subtext uppercase font-bold">
            {sidebarOpen ? '© 2026 KABAKA_SYSTEM_CORP' : 'v1.0'}
          </div>
        </motion.aside>

        <main className="flex-1 flex flex-col overflow-hidden">
          
          <header className="h-16 border-b border-kb-border px-8 flex justify-between items-center bg-kb-card/50 backdrop-blur-sm">
            <div className="flex items-center gap-6">
              <button onClick={toggleSidebar} className="text-kb-subtext hover:text-kb-text transition-colors">
                <Menu size={18} />
              </button>
              <StatusTag label={`Uplink: ${theme.toUpperCase()}_MODE`} status="ok" />
            </div>
            
            <div className="flex items-center gap-6">
              <button 
                onClick={toggleTheme}
                className="p-2 border border-kb-border bg-kb-card hover:border-kb-neon transition-all group"
              >
                {theme === 'dark' ? (
                  <Sun size={16} className="text-kb-subtext group-hover:text-kb-neon" />
                ) : (
                  <Moon size={16} className="text-kb-subtext group-hover:text-kb-neon" />
                )}
              </button>
              
              <div className="flex gap-5">
                <Bell size={18} className="text-kb-subtext hover:text-kb-text cursor-pointer" />
                <Settings size={18} className="text-kb-subtext hover:text-kb-text cursor-pointer" />
              </div>
            </div>
          </header>

          <div className="flex-1 overflow-y-auto p-8 space-y-8">
            <header>
              <h2 className="text-3xl font-black italic tracking-tighter text-kb-text uppercase leading-none">System_Overview</h2>
              <p className="text-[10px] text-kb-subtext tracking-[0.3em] font-black mt-1.5 uppercase opacity-80">Central Intelligence & Distribution Dashboard</p>
            </header>

            <div className="grid grid-cols-4 gap-4">
              <StatCard label="Throughput" val="14.2" unit="k/s" icon={Activity} color="text-kb-neon" />
              <StatCard label="Load_Avg" val="0.24" unit="%" icon={Cpu} color="text-kb-info" />
              <StatCard label="Active_Workers" val="102" unit="qty" icon={Server} color="text-kb-warning" />
              <StatCard label="Errors" val="0.01" unit="%" icon={ShieldAlert} color="text-red-500" />
            </div>

            <div className="grid grid-cols-12 gap-6">
              <section className="col-span-8 bg-kb-card border border-kb-border overflow-hidden">
                <div className="bg-kb-bg/50 px-6 py-3.5 border-b border-kb-border flex justify-between items-center">
                  <h3 className="text-[11px] font-black uppercase tracking-widest flex items-center gap-2">
                    <Database size={14} className="text-kb-neon" /> Active_Queue_Registry
                  </h3>
                </div>
                
                <div className="divide-y divide-kb-border/50">
                  {['orders.process', 'billing.retry', 'logs.aggregate', 'notify.global'].map((topic, i) => (
                    <motion.div 
                      key={topic} 
                      whileHover={{ x: 4, backgroundColor: 'rgba(var(--kb-text), 0.02)' }}
                      onClick={() => setActiveTopic(topic)}
                      className={`p-5 flex items-center justify-between cursor-pointer group transition-all ${activeTopic === topic ? 'border-l-2 border-kb-neon bg-kb-neon/5' : ''}`}
                    >
                      <div className="flex items-center gap-6">
                        <span className="text-[10px] font-bold text-kb-subtext italic">ID_0{i+1}</span>
                        <div>
                          <h4 className="text-sm font-black text-kb-text group-hover:text-kb-neon transition-colors tracking-tight">{topic}</h4>
                          <p className="text-[10px] text-kb-subtext uppercase tracking-tighter mt-1 font-bold">Status: Stable | Shards: 12 | Consumers: 24</p>
                        </div>
                      </div>
                      <div className="flex items-center gap-8">
                        <div className="text-right font-mono">
                          <div className="text-xs font-black text-kb-text">{Math.floor(Math.random() * 5000)} <span className="text-[9px] text-kb-subtext uppercase">Msg/s</span></div>
                          <div className="w-32 h-1 bg-kb-border mt-2 relative overflow-hidden">
                            <motion.div 
                              initial={{ width: 0 }}
                              animate={{ width: `${Math.random() * 100}%` }}
                              transition={{ duration: 1.5, repeat: Infinity, repeatType: 'reverse' }}
                              className="absolute h-full bg-kb-neon/40" 
                            />
                          </div>
                        </div>
                        <ChevronRight size={16} className="text-kb-subtext group-hover:text-kb-neon" />
                      </div>
                    </motion.div>
                  ))}
                </div>
              </section>

              <aside className="col-span-4 space-y-6">
                <div className="bg-kb-card border border-kb-border p-6">
                  <h3 className="text-[10px] font-black uppercase mb-4 text-kb-subtext border-l-2 border-kb-warning pl-2 tracking-widest">System_Intelligence</h3>
                  <div className="space-y-4">
                    {[
                      { t: '14:22:10', m: 'Uplink synchronization complete.', s: 'info' },
                      { t: '14:21:45', m: 'Worker_ID_A02 connected.', s: 'ok' },
                      { t: '14:20:01', m: 'High latency on Topic_02 detected.', s: 'warn' },
                    ].map((log, i) => (
                      <div key={i} className="text-[11px] font-mono flex gap-4 border-l border-kb-border pl-4 pb-2">
                        <span className="text-kb-subtext italic shrink-0">{log.t}</span>
                        <span className={log.s === 'warn' ? 'text-kb-warning' : log.s === 'info' ? 'text-kb-info' : 'text-kb-neon'}>
                          {log.m}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="bg-kb-warning/5 border border-kb-warning/20 p-6 relative overflow-hidden group">
                  <div className="absolute -bottom-4 -right-4 opacity-10 group-hover:scale-110 transition-transform">
                    <Info size={100} className="text-kb-warning" />
                  </div>
                  <h3 className="text-[10px] font-black uppercase mb-2 text-kb-warning tracking-widest">Maintenance_Alert</h3>
                  <p className="text-[10px] leading-relaxed text-kb-warning font-bold opacity-90">Cluster backup scheduled in 14 minutes. All writes will be throttled by 15% during the process.</p>
                </div>
              </aside>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
