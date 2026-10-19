"""B1-real -- PM4Py conformance checking vs the observer, on runs of systems this project did not write.

Input:  correlation-service/src/test/resources/integration/real-project-fixtures.json  (44 runs, 4
        systems; labels from each source's own persisted state or the pre-run plan)
        correlation-service/target/real-project-observer.json  (written by RealProjectIntegrationTest)
        declarations/workflows/*.yaml  (the same specification the observer uses)
Output: results/pm4py_real_baseline.json and a printed table.

Both sides get the same knowledge: the workflow YAML is translated to a Petri net by one generic
rule (below), with no per-system case. Both see the same events, sorted by the source's timestamps.

Two tasks both can attempt:
  T1 state     -- which state is the run in? Observer: its reconstructed status. PM4Py: replay the
                  trace on the net (token game, silent moves allowed) and read the reached marking.
  T2 deviation -- does the run deviate from the specification? Truth: a defect the source is shown
                  by its own state to have committed (fixture expectedFindings). Observer: a standing
                  structural finding. PM4Py: alignment fitness < 1 against the best final marking.
                  Runs still in flight are where the two differ most: a valid unfinished trace is not
                  a deviation, but alignment to a final marking charges it as one.

Translation rule (YAML -> Petri net):
  - one 'running' place per step; one input place per distinct predecessor step (several
    predecessors = AND-join, as KMS settles after both stock and payment);
  - a declared 'started' event is a visible start transition; with none (or started == an outcome
    event) the start is silent;
  - each outcome event (completed -> SUCCESS, failed -> FAILED, named outcomes) is a visible
    transition putting a token on every target of (step, condition) -- several targets = AND-split;
    terminal targets go to an end place; COMPENSATING to the compensation steps; no declared target
    ends the run as COMPLETED / FAILED by condition;
  - a step with no outcome event (a compensation seen only as requested) never finishes: the
    specification names no event that would show it did, so the model does not assume it.
Final markings are the dead markings of the (acyclic) net, enumerated; fitness takes the best.

Limits (printed): the observer column comes from the replay harness (inline detectors, no clock-
driven sweeps), so neither side is shown timing-based findings here; that difference is measured
by the live runs, not by this script.

Run: python tools/pm4py_real_baseline.py
"""
import glob
import json
import os
import sys
from collections import Counter, defaultdict

import pandas as pd
import pm4py
import yaml
from pm4py.objects.petri_net import semantics
from pm4py.objects.petri_net.obj import Marking, PetriNet
from pm4py.objects.petri_net.utils.petri_utils import add_arc_from_to

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "correlation-service", "src", "test", "resources", "integration",
                        "real-project-fixtures.json")
OBSERVED = os.path.join(ROOT, "correlation-service", "target", "real-project-observer.json")
WORKFLOWS = os.path.join(ROOT, "declarations", "workflows")
TERMINALS = ("COMPLETED", "FAILED", "COMPENSATED")
STRUCTURAL = {"FORBIDDEN_POST_TERMINAL", "STATE_REGRESSION", "MULTI_TERMINAL_CONFLICT",
              "INVALID_TRANSITION", "ORPHAN_EVENT", "MISSING_TERMINAL_EVENT"}


# ------------------------------------------------------------------ YAML -> Petri net

def build_net(wf):
    net = PetriNet(wf["name"])
    steps = {s["id"]: s for s in wf["steps"]}
    comps = {c["id"]: c for c in wf.get("compensationSteps", [])}
    every = {**steps, **comps}
    trans = wf.get("transitions", [])

    def place(name):
        p = PetriNet.Place(name)
        net.places.add(p)
        return p

    def transition(name, label):
        t = PetriNet.Transition(name, label)
        net.transitions.add(t)
        return t

    start = place("start")
    ends = {k: place("end_" + k) for k in TERMINALS}
    preds = defaultdict(set)
    for t in trans:
        if t["to"] in every and t["from"] != "STARTED":
            preds[t["to"]].add(t["from"])
    for c in comps:
        preds[c].add("COMPENSATING")
    first = [t["to"] for t in trans if t["from"] == "STARTED"]

    inputs = {sid: {src: place(f"in_{sid}_from_{src}") for src in sorted(preds[sid])} for sid in every}
    running = {sid: place(f"run_{sid}") for sid in every}
    for sid in first:
        inputs[sid]["STARTED"] = start

    for sid, spec in every.items():
        ev = spec.get("events", {}) or {}
        outcomes = []
        if ev.get("completed"):
            outcomes.append(("SUCCESS", ev["completed"]))
        if ev.get("failed"):
            outcomes.append(("FAILED", ev["failed"]))
        for cond, event in (spec.get("outcomes") or {}).items():
            outcomes.append((cond, event))
        outcome_events = {e for _, e in outcomes}
        started = ev.get("started")
        st = transition(f"start_{sid}", started if started and started not in outcome_events else None)
        for p in inputs[sid].values():
            add_arc_from_to(p, st, net)
        add_arc_from_to(st, running[sid], net)

        def produce(t, cond):
            targets = [x["to"] for x in trans if x["from"] == sid and x.get("condition", "SUCCESS") == cond]
            if not targets:
                default = "COMPENSATED" if sid in comps else ("FAILED" if cond == "FAILED" else "COMPLETED")
                targets = [default]
            for target in targets:
                if target in TERMINALS:
                    add_arc_from_to(t, ends[target], net)
                elif target == "COMPENSATING":
                    for c in comps:
                        add_arc_from_to(t, inputs[c]["COMPENSATING"], net)
                else:
                    add_arc_from_to(t, inputs[target][sid], net)

        # No outcome event declared: nothing would show the step finished, so it does not
        # finish in the model either (a compensation seen only as requested stays running).
        for cond, event in outcomes:
            t = transition(f"{sid}_{cond}", event)
            add_arc_from_to(running[sid], t, net)
            produce(t, cond)

    im = Marking({start: 1})
    return net, im, ends, comps, inputs, running


def dead_markings(net, im, limit=5000):
    seen, stack, dead = {im}, [im], []
    while stack and len(seen) < limit:
        m = stack.pop()
        enabled = semantics.enabled_transitions(net, m)
        if not enabled:
            dead.append(m)
        for t in enabled:
            m2 = semantics.execute(t, net, m)
            if m2 not in seen:
                seen.add(m2)
                stack.append(m2)
    return dead


def tau_closure(net, markings):
    seen, stack = set(markings), list(markings)
    while stack:
        m = stack.pop()
        for t in semantics.enabled_transitions(net, m):
            if t.label is None:
                m2 = semantics.execute(t, net, m)
                if m2 not in seen:
                    seen.add(m2)
                    stack.append(m2)
    return seen


def replay_state(model, trace):
    """State reached by the token game, or None when the trace cannot be replayed."""
    net, im, ends, comps, inputs, running = model
    frontier = tau_closure(net, {im})
    for act in trace:
        nxt = set()
        for m in frontier:
            for t in semantics.enabled_transitions(net, m):
                if t.label == act:
                    nxt.add(semantics.execute(t, net, m))
        if not nxt:
            return None
        frontier = tau_closure(net, nxt)
    comp_places = {p for c in comps for p in list(inputs[c].values()) + [running[c]]}
    end_of = {p: k for k, p in ends.items()}
    for m in frontier:
        if not semantics.enabled_transitions(net, m) and all(p in end_of for p in m):
            reached = {end_of[p] for p in m}
            if "COMPENSATED" in reached:
                return "COMPENSATED"
            if "FAILED" in reached:
                return "FAILED"
            return "COMPLETED"
    if any(p in comp_places for m in frontier for p in m):
        return "COMPENSATING"
    return "IN_PROGRESS"


def best_fitness(model, finals, trace):
    if not trace:
        return 0.0
    net, im = model[0], model[1]
    df = pd.DataFrame([{"case:concept:name": "c", "concept:name": a,
                        "time:timestamp": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(seconds=i)}
                       for i, a in enumerate(trace)])
    log = pm4py.format_dataframe(df, case_id="case:concept:name", activity_key="concept:name",
                                 timestamp_key="time:timestamp")
    best = 0.0
    for fm in finals:
        r = pm4py.conformance_diagnostics_alignments(log, net, im, fm)[0]
        best = max(best, r["fitness"])
        if best == 1.0:
            break
    return best


# ------------------------------------------------------------------ run

def load_workflows():
    out = {}
    for path in glob.glob(os.path.join(WORKFLOWS, "*.yaml")):
        wf = yaml.safe_load(open(path, encoding="utf-8"))["workflow"]
        out[wf["name"]] = wf
    return out


def confusion(pairs):
    c = Counter((bool(p), bool(t)) for p, t in pairs)
    tp, fp, fn, tn = c[(True, True)], c[(True, False)], c[(False, True)], c[(False, False)]
    return {"TP": tp, "FP": fp, "FN": fn, "TN": tn}


def main():
    fixtures = json.load(open(FIXTURES, encoding="utf-8"))
    observed = {o["fixture"]: o for o in json.load(open(OBSERVED, encoding="utf-8"))}
    workflows = load_workflows()
    models, finals = {}, {}

    rows = []
    for fx in fixtures:
        wf_name = fx["workflow"]
        if wf_name not in models:
            models[wf_name] = build_net(workflows[wf_name])
            finals[wf_name] = dead_markings(models[wf_name][0], models[wf_name][1])
        trace = [e["eventType"] for e in sorted(fx["events"], key=lambda e: e["timestamp"])]
        obs = observed[fx["fixture"]]
        pm_state = replay_state(models[wf_name], trace)
        fitness = best_fitness(models[wf_name], finals[wf_name], trace)
        rows.append({
            "fixture": fx["fixture"], "system": fx["system"], "truth_state": fx["expectedStatus"],
            "truth_deviation": bool(fx["expectedFindings"]),
            "observer_state": obs["status"],
            "observer_deviation": bool(set(obs["standing"]) & STRUCTURAL),
            "pm4py_state": pm_state if pm_state else "NOT_REPLAYABLE",
            "pm4py_fitness": round(fitness, 4),
            "pm4py_deviation_alignment": fitness < 1.0,
            "pm4py_deviation_replay": pm_state is None,
            "trace": trace,
        })

    systems = sorted({r["system"] for r in rows})
    summary = {}
    print(f"{len(rows)} runs, {len(systems)} systems. Same YAML, same events, timestamps sorted.\n")
    print(f"{'system':10} {'runs':>4} | T1 state correct: {'observer':>8} {'pm4py':>6} | "
          f"T2 deviation flagged (truth): {'obs':>4} {'align':>5} {'replay':>6}")
    for sys_name in systems + ["ALL"]:
        rs = [r for r in rows if sys_name == "ALL" or r["system"] == sys_name]
        s = {
            "runs": len(rs),
            "observer_state_correct": sum(r["observer_state"] == r["truth_state"] for r in rs),
            "pm4py_state_correct": sum(r["pm4py_state"] == r["truth_state"] for r in rs),
            "truth_deviations": sum(r["truth_deviation"] for r in rs),
            "observer_deviation": confusion([(r["observer_deviation"], r["truth_deviation"]) for r in rs]),
            "pm4py_alignment": confusion([(r["pm4py_deviation_alignment"], r["truth_deviation"]) for r in rs]),
            "pm4py_replay": confusion([(r["pm4py_deviation_replay"], r["truth_deviation"]) for r in rs]),
        }
        summary[sys_name] = s
        print(f"{sys_name:10} {s['runs']:>4} | {'':18}{s['observer_state_correct']:>8} {s['pm4py_state_correct']:>6} | "
              f"{'':21}({s['truth_deviations']}) {s['observer_deviation']['TP']+s['observer_deviation']['FP']:>4} "
              f"{s['pm4py_alignment']['TP']+s['pm4py_alignment']['FP']:>5} {s['pm4py_replay']['TP']+s['pm4py_replay']['FP']:>6}")

    print("\nDeviation confusion (ALL):")
    for k in ("observer_deviation", "pm4py_alignment", "pm4py_replay"):
        print(f"  {k:20} {summary['ALL'][k]}")
    print("\nRuns where PM4Py's state differs from the source:")
    for r in rows:
        if r["pm4py_state"] != r["truth_state"]:
            print(f"  {r['fixture'][:46]:46} truth={r['truth_state']:12} pm4py={r['pm4py_state']:14} "
                  f"observer={r['observer_state']:12} fitness={r['pm4py_fitness']}")

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    dst = os.path.join(ROOT, "results", "pm4py_real_baseline.json")
    json.dump({"summary": summary, "runs": rows}, open(dst, "w", encoding="utf-8"), indent=2)
    print(f"\nWritten {dst}")


if __name__ == "__main__":
    sys.exit(main())
