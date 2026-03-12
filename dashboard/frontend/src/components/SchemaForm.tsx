import type Form from "@rjsf/core";
import { withTheme } from "@rjsf/core";
import { Theme as muiTheme } from "@rjsf/mui";
import type { RJSFSchema, UiSchema } from "@rjsf/utils";
import validator from "@rjsf/validator-ajv8";
import { Loader2, Send } from "lucide-react";
import { useRef } from "react";

const ThemedForm = withTheme(muiTheme);

interface SchemaFormProps {
  schema: string;
  onSubmit: (data: unknown) => Promise<void>;
  onChange?: (data: unknown) => void;
  isLoading?: boolean;
}

export const SchemaForm = ({
  schema,
  onSubmit,
  onChange,
  isLoading,
}: SchemaFormProps) => {
  const formRef = useRef<Form>(null);

  let parsedSchema: RJSFSchema;
  try {
    parsedSchema = JSON.parse(schema);
  } catch (_e) {
    return (
      <div className="p-4 bg-red-500/10 border border-red-500/30 text-red-500 rounded-sm font-black text-xs uppercase italic tracking-widest">
        CRITICAL_ERROR: INVALID_JSON_SCHEMA_FORMAT
      </div>
    );
  }

  const uiSchema: UiSchema = {
    "ui:submitButtonOptions": {
      norender: true,
    },
    "ui:options": {
      fullWidth: true,
    },
  };

  const handleManualSubmit = () => {
    if (formRef.current) {
      formRef.current.submit();
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* 
          統一捲動區域：
          這裡包裹了整個 Form 元件，MUI 的所有欄位會在這裡面正常撐開並捲動。
          加上 pt-4 確保第一個欄位不被遮擋。
      */}
      <div className="flex-1 overflow-y-auto custom-scrollbar pr-2 pt-4 min-h-0">
        <ThemedForm
          ref={formRef}
          schema={parsedSchema}
          validator={validator}
          uiSchema={uiSchema}
          onSubmit={({ formData }) => onSubmit(formData)}
          onChange={({ formData }) => onChange?.(formData)}
          autoComplete="off"
          disabled={isLoading}
        />
      </div>

      {/* Publish Button - 固定在最底層，獨立於捲動區域之外 */}
      <div className="shrink-0 pt-6">
        <button
          type="button"
          onClick={handleManualSubmit}
          disabled={isLoading}
          className="group relative w-full h-12 flex items-center justify-center bg-kb-neon text-black font-black uppercase italic tracking-[0.2em] text-xs transition-all hover:brightness-110 disabled:opacity-50 overflow-hidden"
        >
          <div className="absolute inset-0 bg-white/10 opacity-0 group-hover:opacity-100 transition-opacity" />
          <div className="relative z-10 flex items-center gap-3">
            {isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                <span>Publishing...</span>
              </>
            ) : (
              <>
                <Send className="w-4 h-4" />
                <span>Publish</span>
              </>
            )}
          </div>
        </button>
      </div>
    </div>
  );
};
