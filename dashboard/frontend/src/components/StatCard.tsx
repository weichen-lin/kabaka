import { motion } from "framer-motion";
import type { LucideIcon } from "lucide-react";

interface StatCardProps {
  label: string;
  val: string | number;
  unit: string;
  icon: LucideIcon;
  color: string;
  trend?: string;
}

export const StatCard = ({
  label,
  val,
  unit,
  icon: Icon,
  color,
  trend,
}: StatCardProps) => (
  <motion.div
    whileHover={{ scale: 1.02, translateY: -2 }}
    className="bg-kb-card border border-kb-border p-5 relative group overflow-hidden"
  >
    <div className="flex justify-between items-start z-10 relative">
      <Icon
        size={14}
        className="text-kb-subtext group-hover:text-kb-text transition-colors"
      />
      {trend && (
        <span className={`text-[10px] font-bold ${color}`}>{trend}</span>
      )}
    </div>
    <div className="mt-4 flex items-baseline gap-1.5 z-10 relative">
      <span className="text-2xl font-black text-kb-text leading-none">
        {val}
      </span>
      <span className="text-[10px] text-kb-subtext uppercase tracking-tighter font-bold">
        {unit}
      </span>
    </div>
    <div className="mt-1 text-[10px] text-kb-subtext uppercase tracking-widest font-black z-10 relative">
      {label}
    </div>
    <Icon
      className="absolute -bottom-4 -right-4 text-kb-bg brightness-150 opacity-20 rotate-12 transition-transform group-hover:rotate-6 group-hover:scale-110"
      size={70}
    />

    {/* Brutalist accents */}
    <div
      className={`absolute top-0 left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-kb-border to-transparent opacity-50`}
    />
    <div
      className={`absolute bottom-0 right-0 w-[1px] h-full bg-gradient-to-b from-transparent via-kb-border to-transparent opacity-50`}
    />
  </motion.div>
);
