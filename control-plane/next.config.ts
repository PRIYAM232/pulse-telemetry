import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Emit .next/standalone (server.js + traced node_modules) so the Docker
  // runtime stage ships only what the app imports — see control-plane/Dockerfile.
  output: "standalone",
};

export default nextConfig;
