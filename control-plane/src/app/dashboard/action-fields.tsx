"use client";

// Client island for the action-dependent half of the New rule form: the
// action select plus the inputs that only one action needs — sample rate for
// SAMPLE, destination for ROUTE. Rendering them conditionally (instead of the
// old always-visible "(SAMPLE only)" field) means the browser's `required`
// validation can be honest: when the field is visible, it is mandatory.
//
// Everything stays an uncontrolled <form> feeding the createRule Server
// Action; the only client state is which action is selected.

import { useState } from "react";
import { PolicyAction, TargetSignal } from "@/generated/prisma/enums";
import { Field, inputClass } from "./form-ui";

// Suggestions only (datalist allows free text): the destinations the dev
// collector pipeline actually routes on — see the routing connector tables in
// data-plane/config/otelcol-dev.yaml. A destination with no route table entry
// falls through to the default hot pipeline.
const KNOWN_DESTINATIONS = ["cold-storage", "premium-analytics"];

export function ActionFields() {
  const [action, setAction] = useState<string>(PolicyAction.DROP);

  return (
    <>
      <div className="grid grid-cols-2 gap-3">
        <Field label="Action">
          <select
            name="actionType"
            value={action}
            onChange={(e) => setAction(e.target.value)}
            className={inputClass}
          >
            {Object.values(PolicyAction).map((a) => (
              <option key={a} value={a}>{a}</option>
            ))}
          </select>
        </Field>
        <Field label="Signal">
          <select name="targetSignal" defaultValue={TargetSignal.TRACES} className={inputClass}>
            {Object.values(TargetSignal).map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </Field>
      </div>

      {action === PolicyAction.SAMPLE && (
        <Field label="Sample rate (0–1)">
          <input
            name="sampleRate"
            type="number"
            step="0.01"
            min="0.01"
            max="1"
            required
            placeholder="0.1 = keep 10% of traces"
            className={inputClass}
          />
        </Field>
      )}

      {action === PolicyAction.ROUTE && (
        <Field label="Route to destination">
          <input
            name="targetDestination"
            required
            list="pulse-known-destinations"
            placeholder="cold-storage"
            className={inputClass}
          />
          <datalist id="pulse-known-destinations">
            {KNOWN_DESTINATIONS.map((d) => (
              <option key={d} value={d} />
            ))}
          </datalist>
        </Field>
      )}

      {action === PolicyAction.THROTTLE && (
        <div className="grid grid-cols-2 gap-3">
          <Field label="Rate (events/sec)">
            <input
              name="throttleRate"
              type="number"
              step="1"
              min="1"
              required
              placeholder="100"
              className={inputClass}
            />
          </Field>
          {/* Optional on purpose: blank means one global bucket for the rule
              instead of a bucket per attribute value — see createRule. */}
          <Field label="Group by attribute">
            <input
              name="throttleGroupBy"
              placeholder="tenant_id · blank = global"
              className={inputClass}
            />
          </Field>
        </div>
      )}
    </>
  );
}
