import { createTheme } from "@mui/material/styles";

/**
 * Custom MUI Theme generator to support both light and dark modes.
 * Maps Kabaka's Cyberpunk aesthetic to MUI components using HEX for compatibility.
 */
export const getMuiTheme = (mode: "light" | "dark") => {
  const isDark = mode === "dark";

  return createTheme({
    palette: {
      mode,
      primary: {
        main: isDark ? "#ee6b4a" : "#b14c33", // --kb-neon
        contrastText: isDark ? "#000000" : "#ffffff",
      },
      background: {
        default: isDark ? "#3d3d3d" : "#f9f9f9", // --kb-bg
        paper: isDark ? "#474747" : "#ffffff", // --kb-card
      },
      text: {
        primary: isDark ? "#cfd1ce" : "#444444", // --kb-text
        secondary: isDark ? "#c4c6c2" : "#777777", // --kb-subtext
      },
      divider: isDark ? "#575956" : "#e5e5e5", // --kb-border
      error: {
        main: "#f03a3a",
      },
    },
    typography: {
      fontFamily:
        '"Geist", "Geist Fallback", ui-sans-serif, system-ui, sans-serif',
      h1: { fontWeight: 900, fontStyle: "italic" },
      h2: { fontWeight: 900, fontStyle: "italic" },
      h3: { fontWeight: 900, fontStyle: "italic" },
      button: {
        fontWeight: 900,
        textTransform: "uppercase",
        letterSpacing: "0.1em",
      },
    },
    shape: {
      borderRadius: 0,
    },
    components: {
      MuiPaper: {
        styleOverrides: {
          root: {
            backgroundImage: "none",
            border: `1px solid ${isDark ? "#575956" : "#e5e5e5"}`,
          },
        },
      },
      MuiButton: {
        styleOverrides: {
          root: {
            borderRadius: 0,
            boxShadow: "none",
            "&:hover": {
              boxShadow: isDark
                ? "0 0 15px rgba(238, 107, 74, 0.2)"
                : "0 0 10px rgba(177, 76, 51, 0.1)",
            },
          },
        },
      },
      MuiFormLabel: {
        styleOverrides: {
          asterisk: {
            color: "#f03a3a",
          },
        },
      },
      MuiInputLabel: {
        styleOverrides: {
          root: {
            color: isDark ? "#c4c6c2" : "#888888",
            textTransform: "uppercase",
            fontSize: "0.75rem",
            fontWeight: 900,
            letterSpacing: "0.1em",
            transform: "translate(14px, 13px) scale(1)",
            "&.Mui-focused": {
              color: isDark ? "#ee6b4a" : "#b14c33",
            },
            "&.MuiInputLabel-shrink": {
              transform: "translate(14px, -6px) scale(0.75)",
              backgroundColor: isDark ? "#474747" : "#ffffff",
              padding: "0 8px",
              marginLeft: "-4px",
            },
          },
        },
      },
      MuiOutlinedInput: {
        styleOverrides: {
          root: {
            borderRadius: 0,
            backgroundColor: isDark
              ? "rgba(0, 0, 0, 0.2)"
              : "rgba(0, 0, 0, 0.03)",
            "& .MuiOutlinedInput-notchedOutline": {
              borderColor: isDark ? "#575956" : "#e5e5e5",
            },
            "&:hover .MuiOutlinedInput-notchedOutline": {
              borderColor: isDark ? "#ee6b4a" : "#b14c33",
            },
            "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
              borderColor: isDark ? "#ee6b4a" : "#b14c33",
            },
          },
          input: {
            padding: "12px 14px", // Reverted to original padding
            height: "1.4375em",
            color: isDark ? "#cfd1ce" : "#444444",
            "&::placeholder": {
              color: isDark ? "#c4c6c2" : "#999999",
              opacity: 1,
              transform: "translateY(2px)", // Move only the placeholder down
            },
          },
        },
      },
    },
  });
};
