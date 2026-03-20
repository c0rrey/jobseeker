/**
 * Tailwind CSS configuration.
 *
 * This project uses Tailwind CSS v4 which is configured via CSS
 * (@import "tailwindcss" in app/globals.css) rather than through this file.
 * This file is retained for tooling compatibility (editors, linters) and to
 * document the content paths used for class scanning.
 *
 * See: app/globals.css for the active theme configuration.
 */
import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx,js,jsx,mdx}",
    "./components/**/*.{ts,tsx,js,jsx,mdx}",
    "./lib/**/*.{ts,tsx,js,jsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
};

export default config;
