"""
Aggregations for dashboard API responses (mirrors sunbright-dashboard/db.ts logic where possible).
Uses fields present on synced Django models.
"""
from collections import defaultdict

from django.db.models import (
    Avg,
    Count,
    DecimalField,
    Max,
    Min,
    Q,
    Sum,
    Value,
)
from django.db.models.functions import Coalesce
from django.utils import timezone

from dashboard.models import Appointment, CxProject, Door
from dashboard.services.project_service import base_queryset

_ZERO_MONEY = Value(0, output_field=DecimalField(max_digits=15, decimal_places=2))


def _cx_qs(date_from=None, date_to=None):
    qs = CxProject.objects.filter(deleted_at__isnull=True)
    if date_from:
        qs = qs.filter(install_date__gte=date_from)
    if date_to:
        qs = qs.filter(install_date__lte=date_to)
    return qs


def _door_qs(date_from=None, date_to=None):
    qs = Door.objects.filter(deleted_at__isnull=True)
    if date_from:
        qs = qs.filter(create_time__date__gte=date_from)
    if date_to:
        qs = qs.filter(create_time__date__lte=date_to)
    return qs


def _appt_qs(date_from=None, date_to=None):
    qs = Appointment.objects.filter(deleted_at__isnull=True)
    if date_from:
        qs = qs.filter(appointment_datetime__date__gte=date_from)
    if date_to:
        qs = qs.filter(appointment_datetime__date__lte=date_to)
    return qs


_ACTIVE_CLOSED_STAGES = (
    "Deal - Pending Review",
    "Sold",
    "Sold (CRC)",
    "Sold/ Installed",
)
_CANCEL_STAGES = ("Deal Cancelled", "Canceled", "Closed - Not Complete")
_SHOW_STAGES = ("show", "qualified_show", "closed")


def _pct(num, den):
    if not den:
        return 0.0
    return round(100.0 * float(num) / float(den), 1)


def _round_avg(val):
    if val is None:
        return None
    return round(float(val), 1)


def _days_customer_to_install(customer_since, install_date):
    if not customer_since or not install_date or install_date < customer_since:
        return None
    return (install_date - customer_since).days


def _days_span(start, end):
    """Inclusive day delta when both dates exist and end >= start (pipeline milestones)."""
    if not start or not end or end < start:
        return None
    return (end - start).days


def _install_milestone_end(install_completed, install_date):
    """
    Sunbase often has Install Date filled but Install Completed empty; use install date as the
    install milestone so CRC→Install and Install→PTO can still be computed when other dates exist.
    """
    return install_completed or install_date


def _mean_rounded(values):
    vals = [v for v in values if v is not None]
    return _round_avg(sum(vals) / len(vals)) if vals else None


def _avg_install_metrics_by_group(rows, group_field_names):
    """
    Per group: avg days customer_since → install_date (sunbright-dashboard daysToInstall).
    Also clean / non-clean splits for rep-level fields.
    """
    buckets = defaultdict(
        lambda: {"all_s": 0, "all_n": 0, "c_s": 0, "c_n": 0, "nc_s": 0, "nc_n": 0}
    )
    for row in rows:
        key_parts = []
        for f in group_field_names:
            v = row.get(f)
            if isinstance(v, str):
                v = v.strip()
            else:
                v = v if v is not None else ""
            key_parts.append(v)
        key = tuple(key_parts)
        if not key or not key[0]:
            continue
        d = _days_customer_to_install(row.get("customer_since"), row.get("install_date"))
        if d is None:
            continue
        b = buckets[key]
        b["all_s"] += d
        b["all_n"] += 1
        if row.get("is_clean_deal"):
            b["c_s"] += d
            b["c_n"] += 1
        else:
            b["nc_s"] += d
            b["nc_n"] += 1

    out = {}
    for key, b in buckets.items():
        out[key] = {
            "avgDaysToInstall": _round_avg(b["all_s"] / b["all_n"]) if b["all_n"] else None,
            "avgInstallClean": _round_avg(b["c_s"] / b["c_n"]) if b["c_n"] else None,
            "avgInstallNotClean": _round_avg(b["nc_s"] / b["nc_n"]) if b["nc_n"] else None,
        }
    return out


def _clean_deal_analysis_row(qs, is_clean, today):
    sub = qs.filter(is_clean_deal=is_clean)
    total = sub.count()
    installed = sub.filter(install_date__isnull=False).count()
    cancelled = sub.filter(project_category="Cancelled").count()
    active = sub.filter(project_category="Active").count()

    deltas = []
    for cs, ins in sub.filter(install_date__isnull=False, customer_since__isnull=False).values_list(
        "customer_since", "install_date"
    ):
        d = _days_customer_to_install(cs, ins)
        if d is not None:
            deltas.append(d)
    avg_days_install = _round_avg(sum(deltas) / len(deltas)) if deltas else None

    cancel_ages = []
    for cs in sub.filter(project_category="Cancelled", customer_since__isnull=False).values_list(
        "customer_since", flat=True
    ):
        if cs:
            cancel_ages.append((today - cs).days)
    avg_days_cancel = _round_avg(sum(cancel_ages) / len(cancel_ages)) if cancel_ages else None

    return {
        "isCleanDeal": 1 if is_clean else 0,
        "total": total,
        "installed": installed,
        "cancelled": cancelled,
        "active": active,
        "avgDaysToInstall": avg_days_install,
        "avgDaysToCrc": None,
        "avgProjectAge": None,
        "avgDaysToCancel": avg_days_cancel,
    }


def _aggregate_clean_deal_dimension(rows, group_field_names):
    """Mirror sunbright-dashboard getCleanDealByRep/Team/Installer SQL using in-memory aggregation."""
    buckets = defaultdict(
        lambda: {
            "totalProjects": 0,
            "cleanDeals": 0,
            "cleanInstalled": 0,
            "sum_days_clean": 0,
            "cnt_days_clean": 0,
            "sum_days_not_clean": 0,
            "cnt_days_not_clean": 0,
        }
    )

    for row in rows:
        key_parts = []
        skip = False
        for f in group_field_names:
            v = row.get(f) or ""
            if isinstance(v, str):
                v = v.strip()
            if not v:
                skip = True
                break
            key_parts.append(v)
        if skip:
            continue
        key = tuple(key_parts)
        b = buckets[key]
        b["totalProjects"] += 1
        is_clean = bool(row.get("is_clean_deal"))
        if is_clean:
            b["cleanDeals"] += 1
        cs, ins = row.get("customer_since"), row.get("install_date")
        day_inst = _days_customer_to_install(cs, ins)
        if is_clean and ins:
            b["cleanInstalled"] += 1
        if day_inst is not None:
            if is_clean:
                b["sum_days_clean"] += day_inst
                b["cnt_days_clean"] += 1
            else:
                b["sum_days_not_clean"] += day_inst
                b["cnt_days_not_clean"] += 1

    out = []
    camel_map = {"sales_rep": "salesRep", "sales_team": "salesTeam", "installer": "installer"}
    for key, b in buckets.items():
        t = b["totalProjects"]
        c = b["cleanDeals"]
        ci = b["cleanInstalled"]
        item = {
            "totalProjects": t,
            "cleanDeals": c,
            "cleanDealPct": _pct(c, t),
            "cleanInstalled": ci,
            "realizationRatio": _pct(ci, c) if c else 0.0,
            "avgInstallClean": _round_avg(b["sum_days_clean"] / b["cnt_days_clean"])
            if b["cnt_days_clean"]
            else None,
            "avgInstallNotClean": _round_avg(b["sum_days_not_clean"] / b["cnt_days_not_clean"])
            if b["cnt_days_not_clean"]
            else None,
        }
        for i, fname in enumerate(group_field_names):
            item[camel_map.get(fname, fname)] = key[i]
        out.append(item)

    out.sort(key=lambda x: -x["totalProjects"])
    return out[:200]


def get_clean_deals_bundle(date_from=None, date_to=None):
    qs = base_queryset(date_from, date_to)
    today = timezone.now().date()

    analysis = [
        _clean_deal_analysis_row(qs, True, today),
        _clean_deal_analysis_row(qs, False, today),
    ]

    base_cols = ("is_clean_deal", "install_date", "customer_since", "project_category")

    rep_rows = list(qs.exclude(sales_rep="").values("sales_rep", "sales_team", *base_cols))
    by_rep = _aggregate_clean_deal_dimension(rep_rows, ("sales_rep", "sales_team"))

    team_rows = list(qs.exclude(sales_team="").values("sales_team", *base_cols))
    by_team = _aggregate_clean_deal_dimension(team_rows, ("sales_team",))

    inst_rows = list(qs.exclude(installer="").values("installer", *base_cols))
    by_installer = _aggregate_clean_deal_dimension(inst_rows, ("installer",))

    return {"analysis": analysis, "byRep": by_rep, "byTeam": by_team, "byInstaller": by_installer}


def _retention_row(base_qs, group_fields):
    rows = (
        base_qs.values(*group_fields)
        .annotate(
            totalProjects=Count("id"),
            activeProjects=Count("id", filter=Q(project_category="Active")),
            cancelledProjects=Count("id", filter=Q(project_category="Cancelled")),
            onHoldProjects=Count("id", filter=Q(project_category="On Hold")),
            redFlaggedProjects=Count("id", filter=Q(project_category="Red Flagged")),
        )
        .order_by("-totalProjects")[:200]
    )
    out = []
    for r in rows:
        t = r["totalProjects"]
        bad = r["cancelledProjects"] + r["onHoldProjects"] + r["redFlaggedProjects"]
        out.append(
            {
                **{k: r[k] for k in group_fields},
                "totalProjects": t,
                "activeProjects": r["activeProjects"],
                "cancelledProjects": r["cancelledProjects"],
                "onHoldProjects": r["onHoldProjects"],
                "redFlaggedProjects": r["redFlaggedProjects"],
                "cancellationRate": _pct(r["cancelledProjects"], t),
                "onHoldRate": _pct(r["onHoldProjects"], t),
                "netRetentionRate": _pct(t - bad, t) if t else 0.0,
                "avgDaysToCancel": None,
            }
        )
    return out


def get_retention_bundle(date_from=None, date_to=None):
    qs = base_queryset(date_from, date_to)
    return {
        "byRep": _retention_row(qs.exclude(sales_rep=""), ["sales_rep", "sales_team"]),
        "byTeam": _retention_row(qs.exclude(sales_team=""), ["sales_team"]),
        "byInstaller": _retention_row(qs.exclude(installer=""), ["installer"]),
        "byLeadSource": _retention_row(qs.exclude(lead_source=""), ["lead_source"]),
    }


def get_performance_bundle(date_from=None, date_to=None):
    qs = base_queryset(date_from, date_to)
    perf_cols = ("customer_since", "install_date", "is_clean_deal")
    rep_install_rows = list(qs.exclude(sales_rep="").values("sales_rep", "sales_team", *perf_cols))
    rep_install = _avg_install_metrics_by_group(rep_install_rows, ("sales_rep", "sales_team"))
    team_install_rows = list(qs.exclude(sales_team="").values("sales_team", *perf_cols))
    team_install = _avg_install_metrics_by_group(team_install_rows, ("sales_team",))
    inst_install_rows = list(qs.exclude(installer="").values("installer", *perf_cols))
    inst_install = _avg_install_metrics_by_group(inst_install_rows, ("installer",))

    reps = (
        qs.exclude(sales_rep="")
        .values("sales_rep", "sales_team")
        .annotate(
            totalProjects=Count("id"),
            activeProjects=Count("id", filter=Q(project_category="Active")),
            cancelledProjects=Count("id", filter=Q(project_category="Cancelled")),
            onHoldProjects=Count("id", filter=Q(project_category="On Hold")),
            redFlaggedProjects=Count("id", filter=Q(project_category="Red Flagged")),
            disqualifiedProjects=Count("id", filter=Q(project_category="Disqualified")),
            cleanDeals=Count("id", filter=Q(is_clean_deal=True)),
            activePipelineValue=Coalesce(
                Sum("contract_amount", filter=Q(project_category="Active")),
                _ZERO_MONEY,
            ),
            totalContractValue=Coalesce(Sum("contract_amount"), _ZERO_MONEY),
        )
        .order_by("-totalProjects")[:200]
    )
    rep_list = []
    for r in reps:
        t = r["totalProjects"]
        bad = r["cancelledProjects"] + r["onHoldProjects"] + r["redFlaggedProjects"]
        c = r["cleanDeals"]
        ikey = (r["sales_rep"], (r["sales_team"] or "").strip() if r["sales_team"] else "")
        im = rep_install.get(ikey, {})
        rep_list.append(
            {
                "salesRep": r["sales_rep"],
                "salesTeam": r["sales_team"] or "",
                "totalProjects": t,
                "activeProjects": r["activeProjects"],
                "cancelledProjects": r["cancelledProjects"],
                "onHoldProjects": r["onHoldProjects"],
                "redFlaggedProjects": r["redFlaggedProjects"],
                "disqualifiedProjects": r["disqualifiedProjects"],
                "cleanDeals": c,
                "cleanDealPct": _pct(c, t),
                "cancellationRate": _pct(r["cancelledProjects"], t),
                "netRetentionRate": _pct(t - bad, t) if t else 0.0,
                "avgInstallClean": im.get("avgInstallClean"),
                "avgInstallNotClean": im.get("avgInstallNotClean"),
                "avgDaysToInstall": im.get("avgDaysToInstall"),
                "activePipelineValue": float(r["activePipelineValue"] or 0),
                "totalContractValue": float(r["totalContractValue"] or 0),
            }
        )

    teams = (
        qs.exclude(sales_team="")
        .values("sales_team")
        .annotate(
            totalProjects=Count("id"),
            activeProjects=Count("id", filter=Q(project_category="Active")),
            cancelledProjects=Count("id", filter=Q(project_category="Cancelled")),
            onHoldProjects=Count("id", filter=Q(project_category="On Hold")),
            redFlaggedProjects=Count("id", filter=Q(project_category="Red Flagged")),
            cleanDeals=Count("id", filter=Q(is_clean_deal=True)),
            activePipelineValue=Coalesce(
                Sum("contract_amount", filter=Q(project_category="Active")),
                _ZERO_MONEY,
            ),
            totalContractValue=Coalesce(Sum("contract_amount"), _ZERO_MONEY),
        )
        .order_by("-totalProjects")[:200]
    )
    team_list = []
    for r in teams:
        t = r["totalProjects"]
        bad = r["cancelledProjects"] + r["onHoldProjects"] + r["redFlaggedProjects"]
        c = r["cleanDeals"]
        tk = ((r["sales_team"] or "").strip(),)
        tm = team_install.get(tk, {})
        team_list.append(
            {
                "salesTeam": r["sales_team"],
                "totalProjects": t,
                "activeProjects": r["activeProjects"],
                "cancelledProjects": r["cancelledProjects"],
                "onHoldProjects": r["onHoldProjects"],
                "redFlaggedProjects": r["redFlaggedProjects"],
                "cleanDeals": c,
                "cleanDealPct": _pct(c, t),
                "cancellationRate": _pct(r["cancelledProjects"], t),
                "netRetentionRate": _pct(t - bad, t) if t else 0.0,
                "avgDaysToInstall": tm.get("avgDaysToInstall"),
                "activePipelineValue": float(r["activePipelineValue"] or 0),
                "totalContractValue": float(r["totalContractValue"] or 0),
            }
        )

    inst_list = []
    for r in (
        qs.exclude(installer="")
        .values("installer")
        .annotate(
            totalProjects=Count("id"),
            activeProjects=Count("id", filter=Q(project_category="Active")),
            cancelledProjects=Count("id", filter=Q(project_category="Cancelled")),
            onHoldProjects=Count("id", filter=Q(project_category="On Hold")),
            redFlaggedProjects=Count("id", filter=Q(project_category="Red Flagged")),
            cleanDeals=Count("id", filter=Q(is_clean_deal=True)),
            totalContractValue=Coalesce(Sum("contract_amount"), _ZERO_MONEY),
        )
        .order_by("-totalProjects")[:200]
    ):
        t = r["totalProjects"]
        bad = r["cancelledProjects"] + r["onHoldProjects"] + r["redFlaggedProjects"]
        c = r["cleanDeals"]
        ik = ((r["installer"] or "").strip(),)
        im_i = inst_install.get(ik, {})
        inst_list.append(
            {
                "installer": r["installer"],
                "totalProjects": t,
                "activeProjects": r["activeProjects"],
                "cancelledProjects": r["cancelledProjects"],
                "cleanDeals": c,
                "cleanDealPct": _pct(c, t),
                "cancellationRate": _pct(r["cancelledProjects"], t),
                "netRetentionRate": _pct(t - bad, t) if t else 0.0,
                "avgDaysToInstall": im_i.get("avgDaysToInstall"),
                "totalContractValue": float(r["totalContractValue"] or 0),
            }
        )

    return {"reps": rep_list, "teams": team_list, "installers": inst_list}


def get_pipeline_bundle(date_from=None, date_to=None):
    """
    Mirrors sunbright-dashboard getPipelineVelocity + getPipelineVelocityAvg using Project milestone dates.
    """
    qs = base_queryset(date_from, date_to)
    today = timezone.now().date()
    fields = (
        "id",
        "first_name",
        "last_name",
        "sales_rep",
        "sales_team",
        "installer",
        "project_category",
        "is_clean_deal",
        "job_status",
        "customer_since",
        "install_date",
        "site_survey_scheduled",
        "crc_date",
        "permit_approved",
        "install_completed",
        "pto_submitted",
    )
    rows = list(qs.order_by("customer_since").values(*fields)[:5000])

    crc_l, ss_crc_l, permit_l, crc_inst_l = [], [], [], []
    sign_inst_l, inst_pto_l, sign_pto_l = [], [], []
    clean_inst_l, notclean_inst_l = [], []

    velocity = []
    for r in rows:
        cs = r["customer_since"]
        crc = r["crc_date"]
        ss = r["site_survey_scheduled"]
        permit = r["permit_approved"]
        inst_d = r["install_date"]
        inst_c = r["install_completed"]
        inst_done = _install_milestone_end(inst_c, inst_d)
        pto = r["pto_submitted"]

        d_crc = _days_span(cs, crc)
        if d_crc is not None:
            crc_l.append(d_crc)
        d_ss_crc = _days_span(ss, crc)
        if d_ss_crc is not None:
            ss_crc_l.append(d_ss_crc)
        d_perm = _days_span(cs, permit)
        if d_perm is not None:
            permit_l.append(d_perm)
        d_crc_inst = _days_span(crc, inst_done)
        if d_crc_inst is not None:
            crc_inst_l.append(d_crc_inst)
        d_sign_inst = _days_span(cs, inst_d)
        if d_sign_inst is not None:
            sign_inst_l.append(d_sign_inst)
            if r["is_clean_deal"]:
                clean_inst_l.append(d_sign_inst)
            else:
                notclean_inst_l.append(d_sign_inst)
        d_inst_pto = _days_span(inst_done, pto)
        if d_inst_pto is not None:
            inst_pto_l.append(d_inst_pto)
        d_sign_pto = _days_span(cs, pto)
        if d_sign_pto is not None:
            sign_pto_l.append(d_sign_pto)

        project_age = (today - cs).days if cs else None
        velocity.append(
            {
                "id": r["id"],
                "firstName": (r["first_name"] or "").strip() or None,
                "lastName": (r["last_name"] or "").strip() or None,
                "salesRep": r["sales_rep"],
                "salesTeam": r["sales_team"],
                "installer": r["installer"],
                "projectCategory": r["project_category"],
                "isCleanDeal": 1 if r["is_clean_deal"] else 0,
                "jobStatus": r["job_status"],
                "customerSince": cs.isoformat() if cs else None,
                "daysToCrc": d_crc,
                "daysSsToCrc": d_ss_crc,
                "daysToPermit": d_perm,
                "daysToInstall": d_sign_inst,
                "daysCrcToInstall": d_crc_inst,
                "daysInstallToPto": d_inst_pto,
                "daysToPtoSubmitted": d_sign_pto,
                "projectAgeDays": project_age,
            }
        )

    averages = {
        "avgDaysToCrc": _mean_rounded(crc_l),
        "avgDaysSsToCrc": _mean_rounded(ss_crc_l),
        "avgDaysToPermit": _mean_rounded(permit_l),
        "avgDaysCrcToInstall": _mean_rounded(crc_inst_l),
        "avgDaysToInstall": _mean_rounded(sign_inst_l),
        "avgDaysInstallToPto": _mean_rounded(inst_pto_l),
        "avgDaysToPtoSubmitted": _mean_rounded(sign_pto_l),
        "avgInstallClean": _mean_rounded(clean_inst_l),
        "avgInstallNotClean": _mean_rounded(notclean_inst_l),
    }
    return {"velocity": velocity, "averages": averages}


def get_cx_bundle(date_from=None, date_to=None):
    cx = _cx_qs(date_from, date_to)
    total = cx.count()
    reviews = cx.filter(has_review=True).count()
    overview = {
        "totalInstalls": total,
        "reviewsCaptured": reviews,
        "reviewCaptureRate": _pct(reviews, total) if total else 0.0,
        "testimonialPotentials": cx.filter(testimonial_potential=True).count(),
        "testimonialsCompleted": cx.filter(testimonial_done=True).count(),
        "modelHomeCount": cx.filter(model_home_program=True).count(),
        "avgInstallToInspection": _round_avg(cx.aggregate(v=Avg("days_install_to_inspection_passed"))["v"]),
        "avgInstallToPtoSubmitted": _round_avg(cx.aggregate(v=Avg("days_install_to_pto_submitted"))["v"]),
        "avgInstallToPtoApproved": _round_avg(cx.aggregate(v=Avg("days_install_to_pto_approved"))["v"]),
        "avgInstallToReview": _round_avg(cx.aggregate(v=Avg("days_install_to_review"))["v"]),
        "avgInspectionScheduledToPassed": _round_avg(cx.aggregate(v=Avg("days_inspection_scheduled_to_passed"))["v"]),
        "inspectionPassedCount": cx.filter(inspection_passed__isnull=False).count(),
        "ptoSubmittedCount": cx.filter(pto_submitted__isnull=False).count(),
        "ptoApprovedCount": cx.filter(pto_approved__isnull=False).count(),
        "inspectionScheduledCount": cx.filter(inspection_scheduled__isnull=False).count(),
    }

    by_installer = []
    for r in (
        cx.exclude(installer="")
        .values("installer")
        .annotate(
            totalInstalls=Count("id"),
            reviewsCaptured=Count("id", filter=Q(has_review=True)),
            testimonialPotentials=Count("id", filter=Q(testimonial_potential=True)),
            testimonialsCompleted=Count("id", filter=Q(testimonial_done=True)),
            modelHomeCount=Count("id", filter=Q(model_home_program=True)),
            inspectionPassedCount=Count("id", filter=Q(inspection_passed__isnull=False)),
            ptoSubmittedCount=Count("id", filter=Q(pto_submitted__isnull=False)),
            avgInstallToInspection=Avg("days_install_to_inspection_passed"),
            avgInstallToPtoSubmitted=Avg("days_install_to_pto_submitted"),
            avgInstallToPtoApproved=Avg("days_install_to_pto_approved"),
            avgInstallToReview=Avg("days_install_to_review"),
        )
        .order_by("-totalInstalls")[:200]
    ):
        ti = r["totalInstalls"]
        rc = r["reviewsCaptured"]
        by_installer.append(
            {
                "installer": r["installer"],
                "totalInstalls": ti,
                "reviewsCaptured": rc,
                "reviewCaptureRate": _pct(rc, ti),
                "testimonialPotentials": r["testimonialPotentials"],
                "testimonialsCompleted": r["testimonialsCompleted"],
                "modelHomeCount": r["modelHomeCount"],
                "avgInstallToInspection": _round_avg(r.get("avgInstallToInspection")),
                "avgInstallToPtoSubmitted": _round_avg(r.get("avgInstallToPtoSubmitted")),
                "avgInstallToPtoApproved": _round_avg(r.get("avgInstallToPtoApproved")),
                "avgInstallToReview": _round_avg(r.get("avgInstallToReview")),
                "inspectionPassedCount": r["inspectionPassedCount"],
                "ptoSubmittedCount": r["ptoSubmittedCount"],
                "inspectionFailedCount": 0,
            }
        )

    status_breakdown = list(
        cx.values("job_status")
        .annotate(count=Count("id"))
        .order_by("-count")[:100]
    )

    project_list = []
    for row in cx.order_by("-install_date")[:500]:
        project_list.append(
            {
                "id": row.id,
                "firstName": row.first_name,
                "lastName": row.last_name,
                "jobStatus": row.job_status,
                "installer": row.installer,
                "installDate": row.install_date.isoformat() if row.install_date else None,
                "inspectionPassed": row.inspection_passed.isoformat() if row.inspection_passed else None,
                "ptoSubmitted": row.pto_submitted.isoformat() if row.pto_submitted else None,
                "ptoApproved": row.pto_approved.isoformat() if row.pto_approved else None,
                "hasReview": row.has_review,
                "daysInstallToInspectionPassed": row.days_install_to_inspection_passed,
                "daysInstallToPtoApproved": row.days_install_to_pto_approved,
            }
        )

    review_timing = cx.filter(has_review=True, days_install_to_review__isnull=False).aggregate(
        avgDaysToReview=Avg("days_install_to_review"),
        minDaysToReview=Min("days_install_to_review"),
        maxDaysToReview=Max("days_install_to_review"),
    )
    review_timing_out = {
        "avgDaysToReview": _round_avg(review_timing.get("avgDaysToReview")),
        "minDaysToReview": review_timing.get("minDaysToReview"),
        "maxDaysToReview": review_timing.get("maxDaysToReview"),
        "reviewsWithin1Day": cx.filter(days_install_to_review__lte=1).count(),
        "reviewsWithin3Days": cx.filter(days_install_to_review__lte=3).count(),
        "reviewsWithin7Days": cx.filter(days_install_to_review__lte=7).count(),
        "totalReviews": cx.filter(has_review=True).count(),
    }

    timeline_by_installer = [
        {
            "installer": row["installer"],
            "avgInstallToInspection": row["avgInstallToInspection"],
            "avgInstallToPtoSubmitted": row["avgInstallToPtoSubmitted"],
            "avgInstallToPtoApproved": row["avgInstallToPtoApproved"],
            "avgInstallToReview": row["avgInstallToReview"],
        }
        for row in by_installer
    ]

    review_stages = [
        {
            "stage": "Early Post-Install",
            "count": cx.filter(has_review=True, days_install_to_review__lte=1).count(),
        },
        {
            "stage": "Post-Inspection",
            "count": cx.filter(has_review=True, days_install_to_review__gt=1, days_install_to_review__lte=3).count(),
        },
        {
            "stage": "PTO Submit to PTO Approved",
            "count": cx.filter(has_review=True, days_install_to_review__gt=3, days_install_to_review__lte=7).count(),
        },
        {
            "stage": "Inspection to PTO Submit",
            "count": cx.filter(has_review=True, days_install_to_review__gt=7).count(),
        },
    ]
    review_stages = [r for r in review_stages if r["count"] > 0]

    review_details = []
    for row in (
        cx.filter(has_review=True, days_install_to_review__isnull=False)
        .order_by("days_install_to_review", "-review_captured_date")[:500]
    ):
        d = row.days_install_to_review
        if d is None:
            stage = "Unknown"
        elif d <= 1:
            stage = "Early Post-Install"
        elif d <= 3:
            stage = "Post-Inspection"
        elif d <= 7:
            stage = "PTO Submit to PTO Approved"
        else:
            stage = "Inspection to PTO Submit"
        review_details.append(
            {
                "id": row.id,
                "firstName": row.first_name,
                "lastName": row.last_name,
                "installer": row.installer,
                "jobStatus": row.job_status,
                "installDate": row.install_date.isoformat() if row.install_date else None,
                "reviewCapturedDate": row.review_captured_date.isoformat() if row.review_captured_date else None,
                "daysInstallToReview": d,
                "reviewStage": stage,
            }
        )

    return {
        "overview": overview,
        "byInstaller": by_installer,
        "statusBreakdown": status_breakdown,
        "projectList": project_list,
        "timelineByInstaller": timeline_by_installer,
        "reviewTiming": review_timing_out,
        "reviewStages": review_stages,
        "reviewDetails": review_details,
    }


def _manager_overview_counts(appts, doors):
    """Aligns with sunbright-dashboard getManagerOverview (appointment + door rollup)."""
    ta = appts.count()
    sd = appts.filter(stage_category__in=_SHOW_STAGES).count()
    qsd = appts.filter(stage_category__in=("qualified_show", "closed")).count()
    closed = appts.filter(stage_category="closed").count()
    active_closed = appts.filter(deal_stage__in=_ACTIVE_CLOSED_STAGES).count()
    pending = appts.filter(stage_category="pending").count()
    self_set = appts.filter(is_self_set=True).count()
    total_reps = appts.exclude(sales_rep="").values("sales_rep").distinct().count()
    total_teams = appts.exclude(sales_team="").values("sales_team").distinct().count()

    td = doors.count()
    tc = doors.filter(is_contact=True).count()
    canvassers = doors.exclude(canvasser="").values("canvasser").distinct().count()

    return {
        "totalAppointments": ta,
        "totalSitDowns": sd,
        "totalQualifiedSitDowns": qsd,
        "totalClosedDeals": closed,
        "totalActiveClosedDeals": active_closed,
        "totalPendingOutcome": pending,
        "totalSelfSet": self_set,
        "totalReps": total_reps,
        "totalTeams": total_teams,
        "overallSitDownRate": _pct(sd, ta),
        "overallQualifiedSitDownRate": _pct(qsd, ta),
        "overallClosingRate": _pct(closed, sd) if sd else 0.0,
        "overallQualifiedClosingRate": _pct(active_closed, closed) if closed else 0.0,
        "totalDoors": td,
        "totalContacts": tc,
        "overallContactRate": _pct(tc, td) if td else 0.0,
        "totalCanvassers": canvassers,
    }


def get_manager_bundle(date_from=None, date_to=None):
    appts = _appt_qs(date_from, date_to)
    doors = _door_qs(date_from, date_to)

    rep_rows = (
        appts.exclude(sales_rep="")
        .values("sales_rep", "sales_team")
        .annotate(
            totalAppointments=Count("id"),
            sitDowns=Count("id", filter=Q(stage_category__in=_SHOW_STAGES)),
            qualifiedSitDowns=Count(
                "id", filter=Q(stage_category__in=("qualified_show", "closed"))
            ),
            closedDeals=Count("id", filter=Q(stage_category="closed")),
            activeClosedDeals=Count(
                "id",
                filter=Q(deal_stage__in=_ACTIVE_CLOSED_STAGES),
            ),
            cancelledDeals=Count("id", filter=Q(deal_stage__in=_CANCEL_STAGES)),
            selfSetCount=Count("id", filter=Q(is_self_set=True)),
            assignedCount=Count("id", filter=Q(is_self_set=False)),
            pendingOutcome=Count("id", filter=Q(stage_category="pending")),
        )
        .order_by("-closedDeals", "-totalAppointments")[:200]
    )
    rep_performance = []
    for r in rep_rows:
        ta = r["totalAppointments"]
        sd = r["sitDowns"]
        cl = r["closedDeals"]
        acl = r["activeClosedDeals"]
        rep_performance.append(
            {
                "salesRep": r["sales_rep"],
                "salesTeam": r["sales_team"] or "",
                "totalAppointments": ta,
                "sitDowns": sd,
                "qualifiedSitDowns": r["qualifiedSitDowns"],
                "closedDeals": cl,
                "activeClosedDeals": acl,
                "cancelledDeals": r["cancelledDeals"],
                "selfSetCount": r["selfSetCount"],
                "assignedCount": r["assignedCount"],
                "pendingOutcome": r["pendingOutcome"],
                "sitDownRate": _pct(sd, ta),
                "qualifiedSitDownRate": _pct(r["qualifiedSitDowns"], ta),
                "closingRate": _pct(cl, sd) if sd else 0.0,
                "qualifiedClosingRate": _pct(acl, cl) if cl else 0.0,
                "salesTeamDisplay": r["sales_team"] or "",
            }
        )

    team_rows = (
        appts.exclude(sales_team="")
        .values("sales_team")
        .annotate(
            repCount=Count("sales_rep", distinct=True),
            totalAppointments=Count("id"),
            sitDowns=Count("id", filter=Q(stage_category__in=_SHOW_STAGES)),
            qualifiedSitDowns=Count(
                "id", filter=Q(stage_category__in=("qualified_show", "closed"))
            ),
            closedDeals=Count("id", filter=Q(stage_category="closed")),
            activeClosedDeals=Count("id", filter=Q(deal_stage__in=_ACTIVE_CLOSED_STAGES)),
            cancelledDeals=Count("id", filter=Q(deal_stage__in=_CANCEL_STAGES)),
            selfSetCount=Count("id", filter=Q(is_self_set=True)),
            assignedCount=Count("id", filter=Q(is_self_set=False)),
            pendingOutcome=Count("id", filter=Q(stage_category="pending")),
        )
        .order_by("-closedDeals", "-totalAppointments")[:100]
    )
    team_performance = []
    for r in team_rows:
        ta = r["totalAppointments"]
        sd = r["sitDowns"]
        cl = r["closedDeals"]
        acl = r["activeClosedDeals"]
        team_performance.append(
            {
                "salesTeam": r["sales_team"],
                "salesTeamDisplay": r["sales_team"],
                "repCount": r["repCount"],
                "totalAppointments": ta,
                "sitDowns": sd,
                "qualifiedSitDowns": r["qualifiedSitDowns"],
                "closedDeals": cl,
                "activeClosedDeals": acl,
                "cancelledDeals": r["cancelledDeals"],
                "selfSetCount": r["selfSetCount"],
                "assignedCount": r["assignedCount"],
                "pendingOutcome": r["pendingOutcome"],
                "sitDownRate": _pct(sd, ta),
                "qualifiedSitDownRate": _pct(r["qualifiedSitDowns"], ta),
                "closingRate": _pct(cl, sd) if sd else 0.0,
                "qualifiedClosingRate": _pct(acl, cl) if cl else 0.0,
            }
        )

    door_stats = {
        "totalDoors": doors.count(),
        "contacts": doors.filter(is_contact=True).count(),
        "contactRate": _pct(doors.filter(is_contact=True).count(), doors.count())
        if doors.count()
        else 0.0,
    }

    deal_stage_breakdown = list(
        appts.exclude(deal_stage="")
        .values("deal_stage")
        .annotate(count=Count("id"))
        .order_by("-count")[:50]
    )

    pending = []
    for a in appts.filter(stage_category="pending").order_by("appointment_datetime")[:500]:
        pending.append(
            {
                "firstName": a.first_name,
                "lastName": a.last_name,
                "salesRep": a.sales_rep,
                "salesTeam": a.sales_team,
                "dealStage": a.deal_stage,
                "appointmentDateTime": a.appointment_datetime.isoformat()
                if a.appointment_datetime
                else None,
            }
        )

    pq = base_queryset(date_from, date_to)
    overview = {
        "totalProjects": pq.count(),
        "activeProjects": pq.filter(project_category="Active").count(),
        "cancelledProjects": pq.filter(project_category="Cancelled").count(),
    }

    manager_overview = _manager_overview_counts(appts, doors)

    return {
        "overview": overview,
        "managerOverview": manager_overview,
        "repPerformance": rep_performance,
        "teamPerformance": team_performance,
        "doorStats": door_stats,
        "dealStageBreakdown": deal_stage_breakdown,
        "pendingOutcome": pending,
    }
