import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  turbopack: {
    // Set the explicit root to the web/ directory to avoid false workspace
    // detection from the user-level package-lock.json at ~/package-lock.json.
    root: path.resolve(__dirname),
  },
};

export default nextConfig;
