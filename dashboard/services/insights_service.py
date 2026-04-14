"""
AI dashboard insights via an OpenAI-compatible chat completions API (same pattern as sunbright-dashboard
`server/_core/llm.ts`: Manus Forge + Gemini by default).
"""
import json
import os
import urllib.error
import urllib.request
from typing import Any

from dashboard.services.analytics_service import (
    get_clean_deals_bundle,
    get_cx_bundle,
    get_performance_bundle,
    get_retention_bundle,
)
from dashboard.services.project_service import (
    get_cancellation_reasons_breakdown,
    get_on_hold_reasons_breakdown,
    get_overview_metrics,
)


class InsightsLLMError(Exception):
    """Upstream LLM failure or response could not be parsed."""


def _forge_chat_url() -> str:
    base = (os.getenv("BUILT_IN_FORGE_API_URL") or "").strip().rstrip("/")
    if base:
        return f"{base}/v1/chat/completions"
    # forge.manus.im no longer resolves in public DNS; .ai is the working host (same path as sunbright-dashboard intent).
    return "https://forge.manus.ai/v1/chat/completions"


def _forge_api_key() -> str:
    return (os.getenv("BUILT_IN_FORGE_API_KEY") or "").strip()


def _llm_model() -> str:
    return (os.getenv("BUILT_IN_FORGE_MODEL") or "gemini-2.5-flash").strip()


INSIGHTS_JSON_SCHEMA: dict[str, Any] = {
    "name": "insights_response",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "executiveSummary": {
                "type": "string",
                "description": "2-3 paragraph executive summary of overall performance",
            },
            "keyMetrics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "metric": {"type": "string"},
                        "value": {"type": "string"},
                        "status": {"type": "string", "enum": ["good", "warning", "critical"]},
                        "insight": {"type": "string"},
                    },
                    "required": ["metric", "value", "status", "insight"],
                    "additionalProperties": False,
                },
            },
            "repInsights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "repName": {"type": "string"},
                        "strength": {"type": "string"},
                        "improvement": {"type": "string"},
                        "recommendation": {"type": "string"},
                    },
                    "required": ["repName", "strength", "improvement", "recommendation"],
                    "additionalProperties": False,
                },
            },
            "teamInsights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "teamName": {"type": "string"},
                        "strength": {"type": "string"},
                        "improvement": {"type": "string"},
                        "recommendation": {"type": "string"},
                    },
                    "required": ["teamName", "strength", "improvement", "recommendation"],
                    "additionalProperties": False,
                },
            },
            "retentionInsights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "area": {"type": "string"},
                        "finding": {"type": "string"},
                        "recommendation": {"type": "string"},
                        "priority": {"type": "string", "enum": ["high", "medium", "low"]},
                    },
                    "required": ["area", "finding", "recommendation", "priority"],
                    "additionalProperties": False,
                },
            },
            "cxInsights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "area": {"type": "string"},
                        "finding": {"type": "string"},
                        "recommendation": {"type": "string"},
                        "priority": {"type": "string", "enum": ["high", "medium", "low"]},
                    },
                    "required": ["area", "finding", "recommendation", "priority"],
                    "additionalProperties": False,
                },
            },
            "actionItems": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string"},
                        "owner": {"type": "string"},
                        "priority": {"type": "string", "enum": ["high", "medium", "low"]},
                        "expectedImpact": {"type": "string"},
                    },
                    "required": ["action", "owner", "priority", "expectedImpact"],
                    "additionalProperties": False,
                },
            },
        },
        "required": [
            "executiveSummary",
            "keyMetrics",
            "repInsights",
            "teamInsights",
            "retentionInsights",
            "cxInsights",
            "actionItems",
        ],
        "additionalProperties": False,
    },
}


def gather_insights_context(date_from, date_to, user) -> dict[str, Any]:
    perf = get_performance_bundle(date_from, date_to, user)
    cx = get_cx_bundle(date_from, date_to, user)
    retention = get_retention_bundle(date_from, date_to, user)
    clean = get_clean_deals_bundle(date_from, date_to, user)
    return {
        "overview": get_overview_metrics(date_from, date_to, user),
        "repPerformance": (perf.get("reps") or [])[:15],
        "teamPerformance": (perf.get("teams") or [])[:15],
        "cleanDealsByRep": (clean.get("byRep") or [])[:15],
        "retentionByRep": (retention.get("byRep") or [])[:15],
        "retentionByLeadSource": (retention.get("byLeadSource") or [])[:25],
        "cancellationReasons": get_cancellation_reasons_breakdown(date_from, date_to, user)[:25],
        "onHoldReasons": get_on_hold_reasons_breakdown(date_from, date_to, user)[:25],
        "cxOverview": cx.get("overview"),
        "cxByInstaller": (cx.get("byInstaller") or [])[:15],
    }


def _build_user_prompt(ctx: dict[str, Any]) -> str:
    payload = json.dumps(ctx, indent=2, default=str)
    return f"""You are a senior solar industry data analyst for Sunbright Solar USA. Analyze the following dashboard data and provide actionable insights and recommendations for executives, managers, and sales reps.

## Company Data Summary

{payload}

## Instructions
Provide your analysis in the following JSON structure (the API enforces the schema). Be specific with names, numbers, and percentages. Reference actual data points. Each recommendation should be actionable and tied to a specific metric."""


def _invoke_llm(system_prompt: str, user_prompt: str) -> str:
    key = _forge_api_key()
    if not key:
        raise InsightsLLMError("BUILT_IN_FORGE_API_KEY is not set (internal call path).")

    body = {
        "model": _llm_model(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 32768,
        "thinking": {"budget_tokens": 128},
        "response_format": {"type": "json_schema", "json_schema": INSIGHTS_JSON_SCHEMA},
    }
    raw = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        _forge_chat_url(),
        data=raw,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
    )
    timeout = float(os.getenv("BUILT_IN_FORGE_TIMEOUT_SECONDS") or "120")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            parsed = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise InsightsLLMError(f"LLM HTTP {e.code}: {detail}") from e
    except urllib.error.URLError as e:
        hint = ""
        err_s = str(e).lower()
        if "getaddrinfo" in err_s or "11001" in err_s or "name or service not known" in err_s:
            hint = (
                " Could not resolve the API hostname (DNS). If you set BUILT_IN_FORGE_API_URL, check it. "
                "The default host is https://forge.manus.ai (not .im). "
                "Or remove BUILT_IN_FORGE_API_KEY to use rule-based insights without the network."
            )
        raise InsightsLLMError(f"LLM request failed: {e}.{hint}") from e
    except json.JSONDecodeError as e:
        raise InsightsLLMError("LLM returned invalid JSON envelope") from e

    try:
        choices = parsed["choices"]
        msg = choices[0]["message"]
        content = msg["content"]
    except (KeyError, IndexError, TypeError) as e:
        raise InsightsLLMError("LLM response missing choices[0].message.content") from e

    if not isinstance(content, str) or not content.strip():
        raise InsightsLLMError("LLM returned empty content")
    return content


def _metric_status_net_retention(rate: float) -> str:
    if rate >= 82.0:
        return "good"
    if rate >= 68.0:
        return "warning"
    return "critical"


def _metric_status_cancellation(rate: float) -> str:
    if rate <= 8.0:
        return "good"
    if rate <= 18.0:
        return "warning"
    return "critical"


def _metric_status_clean_pct(pct: float) -> str:
    if pct >= 55.0:
        return "good"
    if pct >= 35.0:
        return "warning"
    return "critical"


def _rep_name(row: dict[str, Any]) -> str:
    return str(row.get("salesRep") or row.get("sales_rep") or "Unknown rep").strip() or "Unknown rep"


def _team_name(row: dict[str, Any]) -> str:
    return str(row.get("salesTeam") or row.get("sales_team") or "Unknown team").strip() or "Unknown team"


def _heuristic_insights_from_context(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Same response shape as the LLM path, but computed locally when no Forge key is configured
    (mirrors how sunbright-dashboard is often used: metrics-only until a host-injected key exists).
    """
    ov = ctx.get("overview") or {}
    total = int(ov.get("totalProjects") or 0)
    active = int(ov.get("activeProjects") or 0)
    cancelled = int(ov.get("cancelledProjects") or 0)
    on_hold = int(ov.get("onHoldProjects") or 0)
    clean = int(ov.get("cleanDeals") or 0)
    clean_pct = float(ov.get("cleanDealPct") or 0.0)
    cancel_rate = float(ov.get("cancellationRate") or 0.0)
    net_ret = float(ov.get("netRetentionRate") or 0.0)
    pipe = float(ov.get("activePipelineValue") or 0.0)
    tcv = float(ov.get("totalContractValue") or 0.0)
    avg_install = ov.get("avgDaysToInstall")

    cx = ctx.get("cxOverview") or {}
    cx_installs = int(cx.get("totalInstalls") or 0)
    review_rate = float(cx.get("reviewCaptureRate") or 0.0)

    if total == 0:
        executive = (
            "There are no projects in the current filter window, so portfolio KPIs cannot be compared. "
            "Widen the date range in the header or run a sync if you expect data."
        )
    else:
        p1 = (
            f"In this period the database shows {total} projects ({active} active). "
            f"Clean deals account for {clean_pct:.1f}% of volume ({clean} clean of {total}), "
            f"with a net retention rate of {net_ret:.1f}% and cancellation rate of {cancel_rate:.1f}%."
        )
        p2 = (
            f"Active pipeline value is about ${pipe:,.0f} on roughly ${tcv:,.0f} total contract value in scope. "
        )
        if avg_install is not None:
            p2 += f"Average days from customer date to install is {avg_install} days. "
        if cx_installs:
            p2 += (
                f"Customer experience rows cover {cx_installs} installs with a "
                f"{review_rate:.1f}% review capture rate."
            )
        executive = p1 + "\n\n" + p2.strip()

    key_metrics: list[dict[str, str]] = []
    if total:
        key_metrics.append(
            {
                "metric": "Net retention",
                "value": f"{net_ret:.1f}%",
                "status": _metric_status_net_retention(net_ret),
                "insight": "Share of projects not cancelled, on hold, or red-flagged in this cohort.",
            }
        )
        key_metrics.append(
            {
                "metric": "Clean deal rate",
                "value": f"{clean_pct:.1f}%",
                "status": _metric_status_clean_pct(clean_pct),
                "insight": "Clean installs as a share of all projects in the filter.",
            }
        )
        key_metrics.append(
            {
                "metric": "Cancellation rate",
                "value": f"{cancel_rate:.1f}%",
                "status": _metric_status_cancellation(cancel_rate),
                "insight": f"{cancelled} cancelled projects in this window.",
            }
        )
        key_metrics.append(
            {
                "metric": "On hold",
                "value": str(on_hold),
                "status": "warning" if on_hold > max(3, total // 25) else "good",
                "insight": "Projects currently paused in workflow.",
            }
        )
    if cx_installs:
        key_metrics.append(
            {
                "metric": "CX review capture",
                "value": f"{review_rate:.1f}%",
                "status": "good" if review_rate >= 40 else "warning" if review_rate >= 20 else "critical",
                "insight": "Installs with a captured review vs installs in CX scope.",
            }
        )

    rep_insights: list[dict[str, str]] = []
    for r in (ctx.get("repPerformance") or [])[:6]:
        t = int(r.get("totalProjects") or 0)
        if t < 1:
            continue
        cp = float(r.get("cleanDealPct") or 0.0)
        nr = float(r.get("netRetentionRate") or 0.0)
        cr = float(r.get("cancellationRate") or 0.0)
        rep_insights.append(
            {
                "repName": _rep_name(r),
                "strength": f"Clean deal rate {cp:.1f}% across {t} projects; net retention {nr:.1f}%.",
                "improvement": f"Cancellations at {cr:.1f}% of rep volume — review stalled or at-risk deals.",
                "recommendation": "Pair weekly pipeline reviews with top loss reasons for this rep.",
            }
        )

    team_insights: list[dict[str, str]] = []
    for r in (ctx.get("teamPerformance") or [])[:5]:
        t = int(r.get("totalProjects") or 0)
        if t < 1:
            continue
        cp = float(r.get("cleanDealPct") or 0.0)
        nr = float(r.get("netRetentionRate") or 0.0)
        team_insights.append(
            {
                "teamName": _team_name(r),
                "strength": f"{t} projects with {cp:.1f}% clean rate and {nr:.1f}% net retention.",
                "improvement": "Compare install-cycle delays vs company median to find friction.",
                "recommendation": "Align team coaching on the lowest cohort metric vs branch average.",
            }
        )

    retention_insights: list[dict[str, Any]] = []
    lead_rows = [x for x in (ctx.get("retentionByLeadSource") or []) if int(x.get("totalProjects") or 0) >= 3]
    lead_rows.sort(key=lambda x: float(x.get("netRetentionRate") or 0.0))
    for row in lead_rows[:3]:
        ls = str(row.get("lead_source") or "Lead source").strip() or "Lead source"
        nr = float(row.get("netRetentionRate") or 0.0)
        tp = int(row.get("totalProjects") or 0)
        retention_insights.append(
            {
                "area": f"Lead source: {ls}",
                "finding": f"Net retention {nr:.1f}% over {tp} projects in this filter.",
                "recommendation": "Validate lead quality, handoff SLAs, and pricing fit for this channel.",
                "priority": "high" if nr < 60 else "medium",
            }
        )
    top_cancel = (ctx.get("cancellationReasons") or [])[:1]
    if top_cancel:
        rc = top_cancel[0]
        retention_insights.append(
            {
                "area": "Cancellations",
                "finding": f"Top reason: \"{rc.get('reason')}\" ({rc.get('count')} projects).",
                "recommendation": "Run a focused win/loss review on this reason with sales and operations.",
                "priority": "high" if int(rc.get("count") or 0) > 5 else "medium",
            }
        )
    if not retention_insights:
        retention_insights.append(
            {
                "area": "Retention",
                "finding": "Not enough segmented retention rows in this filter for a channel-level signal.",
                "recommendation": "Expand the date range or ensure lead source fields are populated on sync.",
                "priority": "low",
            }
        )

    cx_insights: list[dict[str, Any]] = []
    if cx_installs:
        cx_insights.append(
            {
                "area": "Reviews",
                "finding": f"Review capture rate is {review_rate:.1f}% across {cx_installs} installs.",
                "recommendation": "Tighten post-install follow-up so more jobs receive a review request within a week.",
                "priority": "high" if review_rate < 25 else "medium" if review_rate < 45 else "low",
            }
        )
    for row in (ctx.get("cxByInstaller") or [])[:4]:
        inst = str(row.get("installer") or "Installer").strip() or "Installer"
        ti = int(row.get("totalInstalls") or 0)
        rr = float(row.get("reviewCaptureRate") or 0.0)
        if ti < 2:
            continue
        cx_insights.append(
            {
                "area": f"Installer: {inst}",
                "finding": f"{ti} installs, {rr:.1f}% review capture.",
                "recommendation": "Share best-practice install closeouts from higher-capture crews with this partner.",
                "priority": "medium" if rr < 35 else "low",
            }
        )
    if not cx_insights:
        cx_insights.append(
            {
                "area": "Customer experience",
                "finding": "No CX install rows in this date window (CX uses install date).",
                "recommendation": "Adjust the header filter or sync CX data if post-install metrics should appear here.",
                "priority": "low",
            }
        )

    actions: list[dict[str, str]] = []
    if total:
        actions.append(
            {
                "action": f"Review the top cancellation reason with managers ({cancel_rate:.1f}% overall rate).",
                "owner": "Sales leadership",
                "priority": "high" if cancel_rate > 15 else "medium",
                "expectedImpact": "Fewer late-stage losses and clearer coaching targets.",
            }
        )
        actions.append(
            {
                "action": "Reconcile on-hold backlog with project owners and set exit dates.",
                "owner": "Operations",
                "priority": "medium" if on_hold else "low",
                "expectedImpact": "Lower working capital risk and clearer pipeline forecasting.",
            }
        )
    if cx_installs and review_rate < 40:
        actions.append(
            {
                "action": "Launch a 7-day post-install review request campaign by installer tier.",
                "owner": "CX / Marketing",
                "priority": "medium",
                "expectedImpact": "Higher review capture and referral-ready customers.",
            }
        )
    if not actions:
        actions.append(
            {
                "action": "Load dashboard data for a meaningful date range, then re-run insights.",
                "owner": "Admin",
                "priority": "low",
                "expectedImpact": "Actionable metrics for the team.",
            }
        )

    return {
        "insightSource": "heuristic",
        "executiveSummary": executive,
        "keyMetrics": key_metrics,
        "repInsights": rep_insights,
        "teamInsights": team_insights,
        "retentionInsights": retention_insights,
        "cxInsights": cx_insights,
        "actionItems": actions[:8],
    }


def generate_dashboard_insights(date_from, date_to, user) -> dict[str, Any]:
    ctx = gather_insights_context(date_from, date_to, user)
    if not _forge_api_key():
        return _heuristic_insights_from_context(ctx)

    system = "You are a senior solar industry data analyst. Provide analysis in valid JSON format only."
    user_prompt = _build_user_prompt(ctx)
    content = _invoke_llm(system, user_prompt)
    try:
        out = json.loads(content)
    except json.JSONDecodeError as e:
        raise InsightsLLMError("Failed to parse structured LLM output as JSON") from e
    if not isinstance(out, dict):
        raise InsightsLLMError("LLM returned a non-object JSON root")
    out["insightSource"] = "llm"
    return out
