import { AnimatePresence, motion } from "framer-motion";
import { X } from "lucide-react";
import { useStore } from "../store/useStore";

export const ConfirmModal = () => {
  const { confirmModal, closeConfirm } = useStore();
  const { isOpen, title, description, message, onConfirm, variant } =
    confirmModal;

  if (!isOpen) return null;

  const handleConfirm = () => {
    onConfirm();
    closeConfirm();
  };

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-[10000] flex items-center justify-center p-4">
        {/* Backdrop */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={closeConfirm}
          className="absolute inset-0 bg-black/80 backdrop-blur-sm"
        />

        {/* Modal Panel */}
        <motion.div
          initial={{ scale: 0.95, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          exit={{ scale: 0.95, opacity: 0 }}
          transition={{ duration: 0.2, ease: "easeOut" }}
          className="relative w-full max-w-[440px] bg-kb-bg border border-kb-border shadow-lg p-6 sm:p-8"
        >
          <div className="flex flex-col gap-4">
            <div className="flex justify-between items-start">
              <div className="space-y-1.5">
                <h3 className="text-lg font-semibold leading-none tracking-tight text-kb-text">
                  {title.replace(/_/g, " ")}
                </h3>
                {description && (
                  <p className="text-[10px] font-black uppercase tracking-widest text-kb-neon/70">
                    {description}
                  </p>
                )}
              </div>
              <button
                type="button"
                onClick={closeConfirm}
                className="rounded-sm opacity-70 transition-opacity hover:opacity-100"
              >
                <X size={18} />
              </button>
            </div>

            <div className="text-sm text-kb-subtext leading-relaxed whitespace-pre-wrap">
              {message}
            </div>

            <div className="flex flex-col-reverse sm:flex-row sm:justify-end gap-3 mt-4">
              <button
                type="button"
                onClick={closeConfirm}
                className="inline-flex h-9 items-center justify-center rounded-md border border-kb-border bg-transparent px-4 py-2 text-sm font-medium transition-colors hover:bg-kb-card hover:text-kb-text"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirm}
                className={`inline-flex h-9 items-center justify-center rounded-md px-4 py-2 text-sm font-medium text-black transition-colors ${
                  variant === "danger"
                    ? "bg-red-500 hover:bg-red-600"
                    : "bg-kb-neon hover:brightness-110"
                }`}
              >
                {variant === "danger" ? "Purge" : "Confirm"}
              </button>
            </div>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};
