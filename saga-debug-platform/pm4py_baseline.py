"""B1 — baseline ngoai: conformance checking cua PM4Py tren cung cac trace da tuong quan.

Cau hoi: mot cong cu process mining chuan, co CUNG kien thuc ve workflow (mo hinh dich tu
order-saga-workflow.yaml), phat hien duoc bao nhieu luot bat thuong ma observer phat hien?
Va no co phan biet duoc "saga dang dung im" voi "saga dang chay binh thuong giua hai buoc"
khi case chua ket thuc khong?

Hai mo hinh:
  - spec:       dich tay tu YAML (buoc, retry, nhanh that bai, chuoi bu tru) -> process tree.
  - discovered: Inductive Miner tren trace cuoi cua cac luot khong co loi (SC01, SC02).

Hai phep do:
  E1 (ket thuc case)  alignment tren trace cuoi cua moi luot; co lech = fitness < 1.
                      So voi nhan kich ban (anomalies_expected khac rong), khong so voi observer.
  E2 (truc tuyen)     moi tien to dang bay (in-flight) cua moi luot, kem thoi gian tien to do
                      dung yen truoc khi event ke tiep toi (hoac toi luc cham diem neu khong co).
                      PM4Py cham tien to theo hai cach:
                        full   = alignment voi final marking: tien to chua xong luon bi coi la lech;
                        prefix = tien to co replay duoc tren net khong: tien to hop le luon khop.
                      Nhan thuc (khong lay tu observer): tien to la "dung im" neu no dung yen
                      lau hon timeout da khai cua buoc dang cho; nguoc lai la "dang bay binh thuong".

Gioi han (in ra cung ket qua):
  - Mo hinh spec do tac gia dich tu YAML; buoc 'started' khong phat thuc te nen de optional,
    retry la vong lap khong chan (process tree khong bieu dien maxAttempts).
  - Nhan "dung im" o E2 dung timeout cua chinh YAML -- cung nguon nguong voi observer. Day la
    dinh nghia cua bat thuong "vang mat" chu khong phai ket qua do cua observer.
  - Offline tren bao cao da ghi, khong phai do live.

Chay: python pm4py_baseline.py results/evaluation_report_settled_20260921.json
"""
import json
import os
import sys
from datetime import datetime

import pandas as pd
import pm4py
import yaml
from pm4py.objects.process_tree.obj import Operator, ProcessTree

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOW = os.path.join(HERE, "correlation-service", "src", "main", "resources",
                        "workflows", "order-saga-workflow.yaml")
HEALTHY_SCENARIOS = ("SC01_", "SC02_")
TERMINAL = {"COMPLETED", "FAILED", "COMPENSATED"}


def _ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _secs(spec):
    spec = str(spec)
    return float(spec[:-2]) / 1000 if spec.endswith("ms") else float(spec.rstrip("s"))


# ---------------------------------------------------------------- model tu YAML

def _leaf(label, parent):
    return ProcessTree(label=label, parent=parent)


def _node(op, parent, build_children):
    n = ProcessTree(operator=op, parent=parent)
    n.children.extend(build_children(n))
    return n


def _optional(label, parent):
    return _node(Operator.XOR, parent, lambda n: [_leaf(None, n), _leaf(label, n)])


def spec_tree(wf):
    """Dich workflow YAML (dang tuyen tinh + nhanh that bai + bu tru) sang process tree."""
    steps = wf["steps"]
    comp_by_id = {c["id"]: c for c in wf.get("compensationSteps", [])}
    fail_to = {t["from"]: t["to"] for t in wf["transitions"] if t.get("condition") == "FAILED"}

    def comp_chain(done, parent):
        # Bu tru cac buoc da xong co khai compensation, theo thu tu nguoc.
        comps = [comp_by_id[s["compensation"]] for s in reversed(done) if s.get("compensation")]
        def kids(n):
            out = []
            for c in comps:
                ev = c["events"]
                out += [_leaf(ev["started"], n), _leaf(ev["completed"], n)]
            return out
        return _node(Operator.SEQUENCE, parent, kids)

    def step_tree(i, parent):
        s = steps[i]
        ev = s["events"]

        def kids(n):
            out = []
            if ev.get("started") and ev["started"] != ev["completed"]:
                out.append(_optional(ev["started"], n))
            if ev.get("retry"):
                out.append(_node(Operator.LOOP, n,
                                 lambda m: [_leaf(None, m), _leaf(ev["retry"], m)]))

            def branches(x):
                fail = _node(Operator.SEQUENCE, x, lambda m: [_leaf(ev["failed"], m)] + (
                    [comp_chain(steps[:i], m)] if fail_to.get(s["id"]) == "COMPENSATING" else []))
                ok = _node(Operator.SEQUENCE, x, lambda m: [_leaf(ev["completed"], m)] + (
                    [step_tree(i + 1, m)] if i + 1 < len(steps) else []))
                return [fail, ok]
            out.append(_node(Operator.XOR, n, branches))
            return out
        return _node(Operator.SEQUENCE, parent, kids)

    return step_tree(0, None)


def step_timeouts(wf):
    """event cuoi cung -> (buoc dang cho, timeout giay) ; dung de gan nhan E2."""
    steps = wf["steps"]
    waits = {}
    for i, s in enumerate(steps):
        nxt = steps[i + 1] if i + 1 < len(steps) else None
        if nxt:
            waits[s["events"]["completed"]] = (nxt["id"], _secs(nxt.get("timeout", "30s")))
        if s["events"].get("retry"):
            waits[s["events"]["retry"]] = (s["id"], _secs(s.get("timeout", "30s")))
    for c in wf.get("compensationSteps", []):
        waits[c["events"]["started"]] = (c["id"], _secs(c.get("timeout", "30s")))
    for t in wf["transitions"]:
        if t.get("condition") == "FAILED" and t["to"] == "COMPENSATING":
            s = next(x for x in steps if x["id"] == t["from"])
            first = next((comp for comp in wf.get("compensationSteps", [])), None)
            if first:
                waits[s["events"]["failed"]] = (first["id"], _secs(first.get("timeout", "30s")))
    return waits


# ---------------------------------------------------------------- log

def traces(report):
    out = []
    for r in report["raw_runs"]:
        evs = sorted(r["raw_events"], key=lambda e: _ts(e["timestamp"]))
        out.append({
            "case": f'{r["scenario_id"]}#{r["run_index"]}',
            "scenario": r["scenario_id"],
            "events": [(e["eventType"], _ts(e["timestamp"])) for e in evs],
            "scored_at": _ts(r["scored_at"]),
            "status": r["saga_status"],
            "expected": r["anomalies_expected"],
            "observer": r["anomalies_actual"],
        })
    return out


def to_log(cases):
    rows = [{"case:concept:name": c, "concept:name": a, "time:timestamp": t}
            for c, evs in cases for a, t in evs]
    df = pd.DataFrame(rows)
    df["time:timestamp"] = pd.to_datetime(df["time:timestamp"], utc=True)
    return pm4py.format_dataframe(df, case_id="case:concept:name",
                                  activity_key="concept:name", timestamp_key="time:timestamp")


def align_fitness(cases, net, im, fm):
    res = pm4py.conformance_diagnostics_alignments(to_log(cases), net, im, fm)
    names = [c for c, _ in cases]
    return {n: r["fitness"] for n, r in zip(names, res)}


def prefix_ok(net, im, prefix):
    """Tien to co replay duoc tren net khong (cho phep transition an xen giua).
    Day la prefix-conformance chinh xac: tim trong khong gian marking, khong qua alignment
    (alignment voi final marking co the hoa chi phi va chon move-on-log cho mot tien to hop le)."""
    from pm4py.objects.petri_net import semantics

    def tau_closure(markings):
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

    frontier = tau_closure({im})
    for act in prefix:
        nxt = set()
        for m in frontier:
            for t in semantics.enabled_transitions(net, m):
                if t.label == act:
                    nxt.add(semantics.execute(t, net, m))
        if not nxt:
            return False
        frontier = tau_closure(nxt)
    return True


# ---------------------------------------------------------------- do

def confusion(pairs):
    tp = sum(1 for p, t in pairs if p and t)
    fp = sum(1 for p, t in pairs if p and not t)
    fn = sum(1 for p, t in pairs if not p and t)
    tn = sum(1 for p, t in pairs if not p and not t)
    return {"TP": tp, "FP": fp, "FN": fn, "TN": tn,
            "recall": round(tp / (tp + fn), 3) if tp + fn else None,
            "precision": round(tp / (tp + fp), 3) if tp + fp else None}


def main(path):
    report = json.load(open(path, encoding="utf-8"))
    wf = yaml.safe_load(open(WORKFLOW, encoding="utf-8"))["workflow"]
    runs = traces(report)

    models = {}
    tree = spec_tree(wf)
    models["spec"] = pm4py.convert_to_petri_net(tree)
    healthy = [(r["case"], r["events"]) for r in runs if r["scenario"].startswith(HEALTHY_SCENARIOS)]
    models["discovered"] = pm4py.discover_petri_net_inductive(to_log(healthy))

    out = {"source_report": os.path.basename(path), "spec_tree": str(tree),
           "e1_end_of_case": {}, "e2_in_flight": {}}

    # ---- E1
    final = [(r["case"], r["events"]) for r in runs]
    truth = {r["case"]: bool(r["expected"]) for r in runs}
    observer = {r["case"]: bool(r["observer"]) for r in runs}
    e1 = {"observer": confusion([(observer[c], truth[c]) for c, _ in final])}
    per_run = {c: {"truth": truth[c], "observer": observer[c]} for c, _ in final}
    for name, (net, im, fm) in models.items():
        fit = align_fitness(final, net, im, fm)
        e1[f"pm4py_{name}"] = confusion([(fit[c] < 1.0, truth[c]) for c, _ in final])
        for c in fit:
            per_run[c][f"pm4py_{name}_fitness"] = round(fit[c], 4)
    e1["per_run"] = per_run
    out["e1_end_of_case"] = e1

    # ---- E2: moi tien to dang bay
    waits = step_timeouts(wf)
    snaps = []
    for r in runs:
        evs = r["events"]
        for k in range(1, len(evs) + 1):
            last = evs[k - 1][0]
            if k == len(evs) and r["status"] in TERMINAL:
                continue  # case da ket thuc, khong con "dang bay"
            if last not in waits:
                continue
            until = evs[k][1] if k < len(evs) else r["scored_at"]
            idle = (until - evs[k - 1][1]).total_seconds()
            step, timeout = waits[last]
            snaps.append({"snapshot": f'{r["case"]}@{k}', "case": r["case"],
                          "prefix": [a for a, _ in evs[:k]], "waiting_for": step,
                          "timeout_s": timeout, "idle_s": round(idle, 3),
                          "stalled": idle > timeout, "events": evs[:k]})
    prefixes = [(s["snapshot"], s["events"]) for s in snaps]
    e2 = {"snapshots": len(snaps), "stalled": sum(s["stalled"] for s in snaps)}
    for name, (net, im, fm) in models.items():
        fit = align_fitness(prefixes, net, im, fm)
        ok = {s["snapshot"]: prefix_ok(net, im, s["prefix"]) for s in snaps}
        e2[f"pm4py_{name}_full"] = confusion([(fit[s["snapshot"]] < 1.0, s["stalled"]) for s in snaps])
        e2[f"pm4py_{name}_prefix"] = confusion([(not ok[s["snapshot"]], s["stalled"]) for s in snaps])
        for s in snaps:
            s[f"pm4py_{name}_full_fitness"] = round(fit[s["snapshot"]], 4)
            s[f"pm4py_{name}_prefix_replayable"] = ok[s["snapshot"]]
    # Cac cap tien to giong het nhau nhung khac nhan: bat ky bo kiem tra control-flow
    # khong co thoi gian nao cung buoc phai cham chung nhu nhau.
    by_prefix = {}
    for s in snaps:
        by_prefix.setdefault(tuple(s["prefix"]), set()).add(s["stalled"])
    e2["prefixes_with_both_labels"] = [list(p) for p, v in by_prefix.items() if len(v) == 2]
    e2["detail"] = [{k: v for k, v in s.items() if k != "events"} for s in snaps]
    out["e2_in_flight"] = e2

    print(f"Nguon: {path}  ({len(runs)} luot)")
    print(f"Mo hinh spec: {tree}")
    print("\nE1 — trace cuoi, so voi nhan kich ban (co bat thuong hay khong):")
    for k, v in e1.items():
        if k != "per_run":
            print(f"  {k:22s} {v}")
    print(f"\nE2 — {e2['snapshots']} tien to dang bay, {e2['stalled']} la dung im qua timeout:")
    for k, v in e2.items():
        if k.startswith("pm4py_"):
            print(f"  {k:28s} {v}")
    print("  Tien to xuat hien voi CA HAI nhan (dung im / dang bay binh thuong):")
    for p in e2["prefixes_with_both_labels"]:
        print(f"    {p}")
    print("\nGioi han: xem docstring. Observer khong co cot E2 vi bao cao khong luu verdict tung"
          " tien to; verdict cua no o muc luot nam o E1.")

    dst = os.path.join(HERE, "results", "pm4py_baseline_" + os.path.basename(path).replace(
        "evaluation_report_", ""))
    json.dump(out, open(dst, "w", encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print(f"\nDa ghi {dst}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else
         os.path.join(HERE, "results", "evaluation_report_settled_20260921.json"))
