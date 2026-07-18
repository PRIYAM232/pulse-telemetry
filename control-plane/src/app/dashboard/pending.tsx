"use client";

// Tiny client-side helpers for the otherwise fully server-rendered dashboard.

import { useFormStatus } from "react-dom";

/**
 * Submit button that dims and locks while its enclosing form's Server Action
 * is in flight. Must live inside a <form>; useFormStatus reads the nearest
 * form's pending state.
 */
export function SubmitButton({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  const { pending } = useFormStatus();
  return (
    <button
      type="submit"
      disabled={pending}
      className={`${className ?? ""} transition-opacity disabled:cursor-wait disabled:opacity-40`}
    >
      {children}
    </button>
  );
}
