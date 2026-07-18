"use client";

// Re-renders the dashboard's server components on an interval so the metric
// banner tracks the collectors' 10s heartbeat without a manual reload.
// router.refresh() refetches the RSC payload in place — client state (form
// inputs, scroll) is preserved, unlike a full navigation.

import { useRouter } from "next/navigation";
import { useEffect } from "react";

export function AutoRefresh({ intervalMs }: { intervalMs: number }) {
  const router = useRouter();

  useEffect(() => {
    const timer = setInterval(() => router.refresh(), intervalMs);
    return () => clearInterval(timer);
  }, [router, intervalMs]);

  return null;
}
