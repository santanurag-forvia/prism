# dashboard/views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from datetime import date
from calendar import month_name
from django.db import connection

# ---------------------------------------------------------------------
#  Utility helpers
# ---------------------------------------------------------------------
def dict_fetchall(sql, params=None):
    """Execute raw SQL and return list of dicts."""
    with connection.cursor() as c:
        c.execute(sql, params or [])
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in c.fetchall()]


def last_12_months_list():
    """Return list of last 12 months for dropdowns and charts."""
    today = date.today()
    months = []
    for i in range(12):
        m = (today.month - i - 1) % 12 + 1
        y = today.year - ((today.month - i - 1) // 12)
        months.append({"iso": f"{y}-{m:02d}", "label": f"{month_name[m]} {y}"})
    return list(reversed(months))


# ---------------------------------------------------------------------
#  Main dashboard view (role-aware)
#  (keeps original behavior but PDL section now renders hours-only visuals)
# ---------------------------------------------------------------------

from datetime import date

def dashboard_view(request):
    print("Rendering dashboard for user:", request.session.get('ldap_username', 'Anonymous'))

    # Use session role only (fallback to EMPLOYEE)
    user_role = request.session.get("role", "EMPLOYEE")
    user_ldap = request.session['ldap_username'] or None
    creator_cn = request.session.get("cn") or user_ldap or ""

    is_pdl = user_role in ("PDL", "ADMIN")
    is_manager = user_role in ("TEAM_LEAD", "COE_LEADER")

    year = int(request.GET.get("year", date.today().year))
    selected_month = request.GET.get("month", date.today().strftime("%Y-%m"))

    context = {
        "year": year,
        "available_years": list(range(year - 2, year + 1)),
        "selected_month": selected_month,
        "last_12_months": last_12_months_list(),
    }

    context["user_stats"] = compute_user_stats(user_ldap, selected_month)
    context["user_allocations"] = list_user_allocations(user_ldap)

    if is_manager or is_pdl:
        reportees = get_reportees_for_manager(user_ldap)
        context["reportees"] = reportees
        context["manager_totals"] = compute_manager_totals(reportees, year)
        context["manager_view"] = bool(reportees)
    else:
        context["manager_view"] = False

    if is_pdl:
        context["pdl_view"] = True
        context["pdl_totals"] = 10 #compute_pdl_totals(year, request)
        context["pdl_variances"] = []
    else:
        context["pdl_view"] = False

    return render(request, "dashboard/home.html", context)

# ---------------------------------------------------------------------
#  INDIVIDUAL DATA FUNCTIONS
# ---------------------------------------------------------------------
def compute_user_stats(user_ldap, month_iso):
    """Compute individual utilization for a given month."""
    year, mon = map(int, month_iso.split("-"))
    sql = """
        SELECT SUM(total_hours) AS total
        FROM monthly_allocation_entries
        WHERE user_ldap = %s
          AND DATE_FORMAT(month_start, '%%Y-%%m') = %s
    """
    rows = dict_fetchall(sql, (user_ldap, f"{year}-{mon:02d}"))
    total = float(rows[0]["total"] or 0)
    max_hours = 183.75  # or from monthly_hours_limit
    util = round((total / max_hours * 100) if max_hours else 0, 1)
    return {
        "this_month_hours": total,
        "utilization_percent": util,
        "remaining_hours": round(max(max_hours - total, 0), 2),
    }

def list_user_allocations(user_ldap):
    """
    Retrieve the top 10 project allocations for a given user.

    This function queries the `monthly_allocation_entries` table to find all allocations
    for the specified user, joins with the `projects` table to get project names, and
    sums the total hours allocated per project. Results are ordered by total hours in
    descending order and limited to the top 10 projects.

    Args:
        user_ldap (str): The LDAP username or identifier for the user.

    Returns:
        List[Dict]: A list of dictionaries, each containing:
            - project_id (int): Unique identifier of the project.
            - project_name (str): Name of the project.
            - total_hours (float): Total hours allocated to the user for the project.

    Example:
        allocations = list_user_allocations("john.doe")
        for alloc in allocations:
            print(alloc["project_name"], alloc["total_hours"])

    Raises:
        Any database errors will propagate from the underlying dict_fetchall helper.
        If no allocations are found, returns an empty list.
    """
    sql = """
        SELECT m.project_id, p.name AS project_name, SUM(m.total_hours) AS total_hours
        FROM monthly_allocation_entries m
        LEFT JOIN projects p ON p.id = m.project_id
        WHERE m.user_ldap = %s
        GROUP BY m.project_id, p.name
        ORDER BY total_hours DESC
        LIMIT 10
    """
    return dict_fetchall(sql, (user_ldap,))


# ---------------------------------------------------------------------
#  MANAGER DATA FUNCTIONS
# ---------------------------------------------------------------------
def get_reportees_for_manager(manager_ldap):
    """
    Retrieve direct reportees for a given manager from the LDAP directory.

    This function queries the `ldap_directory` table to find users whose `manager_dn`
    matches the LDAP DN of the specified manager. It returns a list of dictionaries
    containing the LDAP username, common name, and title for each reportee.

    Args:
        manager_ldap (str): The LDAP username or identifier of the manager.

    Returns:
        List[Dict]: A list of dictionaries, each containing:
            - user_ldap (str): The LDAP username of the reportee.
            - name (str): The common name (CN) of the reportee.
            - title (str): The title of the reportee.

    SQL Query Details:
        - Selects username, CN, and title from `ldap_directory`.
        - Filters users whose `manager_dn` matches the LDAP DN of the given manager.
        - Uses a subquery to resolve the manager's LDAP DN.

    Example:
        reportees = get_reportees_for_manager("manager.ldap")
        for r in reportees:
            print(r["name"], r["title"])

    Raises:
        Any database errors will propagate from the underlying dict_fetchall helper.
        If no reportees are found, returns an empty list.
    """
    sql = """
        SELECT ld.username AS user_ldap, ld.cn AS name, ld.title
        FROM ldap_directory ld
        WHERE ld.manager_dn = (
            SELECT ldap_dn FROM ldap_directory WHERE username = %s LIMIT 1
        )
    """
    return dict_fetchall(sql, (manager_ldap,))


def compute_manager_totals(reportees, year):
    """
    Aggregate team-level allocation and billing ratio for a manager's reportees.

    This function calculates the total hours allocated to all direct reportees of a manager
    for a given year. It also computes the billing ratio, which is the percentage of total
    allocated hours compared to the maximum possible hours (183.75 per reportee). The result
    includes the total allocation, billing ratio, and a placeholder for open allocations.

    Args:
        reportees (List[Dict]): List of dictionaries representing reportees, each containing at least 'user_ldap'.
        year (int): The year for which allocations are aggregated.

    Returns:
        Dict: A dictionary containing:
            - team_alloc (float): Total hours allocated to the team.
            - billing_ratio (str): Billing ratio as a percentage string.
            - open_allocations (int): Placeholder for open allocations (currently always 0).

    SQL Query Details:
        - Sums total_hours from monthly_allocation_entries for all reportees in the given year.

    Example:
        totals = compute_manager_totals(reportees, 2024)
        print(totals["team_alloc"], totals["billing_ratio"])

    Raises:
        Any database errors will propagate from the underlying dict_fetchall helper.
        If reportees is empty, returns zeroed values.
    """
    if not reportees:
        return {"team_alloc": 0, "billing_ratio": "0%", "open_allocations": 0}

    udns = [r["user_ldap"] for r in reportees if r.get("user_ldap")]
    placeholders = ",".join(["%s"] * len(udns))
    sql = f"""
        SELECT SUM(total_hours) AS total_alloc
        FROM monthly_allocation_entries
        WHERE user_ldap IN ({placeholders})
          AND YEAR(month_start) = %s
    """
    rows = dict_fetchall(sql, udns + [year])
    total_alloc = float(rows[0]["total_alloc"] or 0)
    br = round((total_alloc / (len(udns) * 183.75) * 100) if udns else 0, 1)
    return {
        "team_alloc": total_alloc,
        "billing_ratio": f"{br}%",
        "open_allocations": 0,
    }


# ---------------------------------------------------------------------
#  PDL helper: resolve creators
# ---------------------------------------------------------------------
def resolve_possible_creators_from_session(request):
    """
    Return a list of possible strings that might match prism_wbs.creator for the current logged-in user.

    This function generates a list of possible creator identifiers based on the user's session and profile.
    It considers the user's common name (cn), username, display name, and related LDAP directory entries.
    The output is used to match records in the database where the creator field may have different formats.

    Args:
        request (HttpRequest): The Django request object containing session and user info.

    Returns:
        List[str]: A list of possible creator strings for the current user.
    """
    possible = []  # List to collect possible creator strings

    cn = request.session.get('cn')  # Get common name from session
    if cn:
        cn = str(cn).strip()  # Ensure it's a string and strip whitespace
        possible.append(cn)  # Add original CN
        parts = cn.split()  # Split CN into parts
        if len(parts) >= 2:
            first = parts[0]
            rest = parts[1:]
            reversed_form = " ".join(rest + [first])  # Reverse order
            if reversed_form not in possible:
                possible.append(reversed_form)  # Add reversed CN if not present
        cap = " ".join([p.capitalize() for p in cn.split()])  # Capitalize each part
        if cap not in possible:
            possible.append(cap)  # Add capitalized CN

    username = request.session.get('ldap_username') or getattr(request.user, 'ldap_username', None)  # Get username from session or user object
    if username:
        username = str(username).strip()
        if username not in possible:
            possible.append(username)  # Add username
        if '@' in username:
            local = username.split('@')[0]  # Get local part before @
            if '.' in local:
                candidate = " ".join([p.capitalize() for p in local.split('.')])  # Capitalize dot-separated parts
                if candidate not in possible:
                    possible.append(candidate)  # Add candidate
            else:
                candidate = local.capitalize()
                if candidate not in possible:
                    possible.append(candidate)  # Add capitalized local part

    try:
        display = request.user.get_full_name()  # Try to get user's display name
    except Exception:
        display = None
    if display:
        display = display.strip()
        if display and display not in possible:
            possible.append(display)  # Add display name if not present

    try:
        if username:
            # Query LDAP directory for matching CN, username, or mail
            rows = dict_fetchall(
                "SELECT cn, username, mail FROM ldap_directory WHERE username=%s OR mail=%s LIMIT 5",
                (username, username)
            )
            for r in rows:
                if r.get('cn') and r['cn'] not in possible:
                    possible.append(r['cn'])  # Add CN from LDAP
                if r.get('mail') and r['mail'] not in possible:
                    possible.append(r['mail'])  # Add mail from LDAP
                if r.get('username') and r['username'] not in possible:
                    possible.append(r['username'])  # Add username from LDAP
    except Exception:
        pass  # Ignore errors in LDAP lookup

    out = []  # Final output list, deduplicated
    for p in possible:
        if p and p not in out:
            out.append(p)  # Add unique, non-empty values

    return out  # Return list of possible creator strings


# ---------------------------------------------------------------------
#  PDL totals and helpers (original functions retained)
# ---------------------------------------------------------------------
def compute_pdl_totals(year, request):
    """
    Returns ytd_hours for the creator's IOMs.
    The original implementation also computed cost; we keep returning the same keys
    but the template will only render hours (we intentionally do not display cost).
    """
    creator_candidates = resolve_possible_creators_from_session(request)
    if not creator_candidates:
        return {"ytd_hours": 0, "ytd_cost": 0.0, "month_cost": 0.0, "month_estimate": 0.0}

    placeholders = ",".join(["%s"] * len(creator_candidates))
    sql = f"""
        SELECT
          COALESCE(SUM(CAST(total_hours AS DECIMAL(18,2))),0) AS total_hours,
          COALESCE(SUM(CAST(total_hour_costs_local AS DECIMAL(18,2))),0) AS total_cost_local
        FROM prism_master_wor
        WHERE year=%s AND creator IN ({placeholders})
    """
    params = [str(year)] + creator_candidates
    rows = dict_fetchall(sql, params)
    if not rows:
        return {"ytd_hours": 0, "ytd_cost": 0.0, "month_cost": 0.0, "month_estimate": 0.0}
    total_hours = float(rows[0].get('total_hours') or 0)
    total_cost_local = float(rows[0].get('total_cost_local') or 0)
    return {
        "ytd_hours": total_hours,
        "ytd_cost": total_cost_local,
        "month_cost": 0.0,
        "month_estimate": 0.0
    }


# ---------------------------------------------------------------------
#  NEW: PDL hours series (monthly consumed vs estimated/allotted)
#  Endpoint: returns JSON { labels: [...], consumed: [...], estimated: [...] }
#  Uses prism_master_wor per-month columns (jan..dec) for consumed and total_hours for estimated.
#  Assumption: prism_master_wor contains the monthly columns jan..dec and total_hours.
# ---------------------------------------------------------------------

def pdl_hours_series(request, year):
    """
    Returns monthly series (consumed & estimated) for the logged-in user's created IOMs.
    Query param: ?year=YYYY (path param is also accepted by your urls if configured)
    """
    creator_candidates = resolve_possible_creators_from_session(request)
    labels = [m.capitalize() for m in ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]]
    consumed = [0.0]*12
    estimated = [0.0]*12

    if not creator_candidates:
        return JsonResponse({"labels": labels, "consumed": consumed, "estimated": estimated})

    placeholders = ",".join(["%s"] * len(creator_candidates))
    # select monthly columns if present
    select_cols = ", ".join([f"COALESCE({c},0) as {c}" for c in ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]])
    sql = f"SELECT {select_cols}, CAST(total_hours AS DECIMAL(18,2)) AS total_hours FROM prism_master_wor WHERE year=%s AND creator IN ({placeholders})"
    params = [str(year)] + creator_candidates
    rows = dict_fetchall(sql, params)

    if rows:
        # accumulate consumed months
        for r in rows:
            for i, c in enumerate(["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]):
                consumed[i] += float(r.get(c) or 0.0)
            total_hours = float(r.get('total_hours') or 0.0)
            # if per-month estimate not present, distribute total_hours evenly across 12 months
            if total_hours:
                per_month = total_hours / 12.0
                for i in range(12):
                    estimated[i] += per_month

    # return floats (JSON friendly)
    consumed = [round(x, 2) for x in consumed]
    estimated = [round(x, 2) for x in estimated]
    return JsonResponse({"labels": labels, "consumed": consumed, "estimated": estimated})


# ---------------------------------------------------------------------
#  NEW: PDL program/department breakdown
#  Endpoint: returns JSON { items: [ { program, department, allotted, consumed } ... ] }
#  Query params: ?year=YYYY (&month=1..12 optionally) (&dept=DepartmentName optional)
#  NOTE: This uses prism_wbs for aggregation — adjust table/column names if your schema differs.
# ---------------------------------------------------------------------

def pdl_program_breakdown(request):
    """
    Return program-wise or department-wise breakdown for PDL's created IOMs.
    Query params: year (required), month (optional numeric 1-12), dept (optional)
    """
    year = request.GET.get('year') or date.today().year
    month = request.GET.get('month')  # numeric '1'..'12' or None
    dept = request.GET.get('dept')

    creator_candidates = resolve_possible_creators_from_session(request)
    if not creator_candidates:
        return JsonResponse({"items": []})

    placeholders = ",".join(["%s"] * len(creator_candidates))

    params = [str(year)] + creator_candidates
    # If month provided we try to sum that single month column from prism_master_wor (safer route)
    if month:
        try:
            mnum = int(month)
            month_cols = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
            month_col = month_cols[mnum-1]
            # Assumption: prism_master_wor or prism_wbs has a 'program' and 'department' column.
            sql = f"""
                SELECT COALESCE(pm.program, '') AS program,
                       COALESCE(pm.department, '') AS department,
                       COALESCE(SUM(COALESCE(pm.{month_col},0)),0) AS consumed,
                       0 AS allotted
                FROM prism_master_wor pm
                WHERE pm.year=%s AND pm.creator IN ({placeholders})
            """
            if dept:
                sql += " AND pm.department = %s"
                params.append(dept)
            sql += " GROUP BY COALESCE(pm.program,'') ORDER BY COALESCE(pm.program,'')"
            rows = dict_fetchall(sql, params)
            # Note: allotted isn't directly available in prism_master_wor month column; left as 0 unless you have source for allotted
            items = [{"program": r["program"] or "Unknown", "department": r["department"] or "", "allotted": float(r.get("allotted") or 0), "consumed": float(r.get("consumed") or 0)} for r in rows]
        except Exception:
            items = []
    else:
        # YTD department aggregates (try prism_wbs which is likely to contain allotted/consumed per department)
        sql = f"""
            SELECT COALESCE(wb.program, wb.department, '') AS program,
                   COALESCE(wb.department, '') AS department,
                   COALESCE(SUM(COALESCE(wb.allotted_hours,0)),0) AS allotted,
                   COALESCE(SUM(COALESCE(wb.consumed_hours,0)),0) AS consumed
            FROM prism_wbs wb
            WHERE wb.year=%s AND wb.creator IN ({placeholders})
        """
        if dept:
            sql += " AND wb.department = %s"
            params.append(dept)
        sql += " GROUP BY COALESCE(wb.program, wb.department) ORDER BY COALESCE(wb.program, wb.department)"
        rows = dict_fetchall(sql, params)
        items = [{"program": r.get("program") or r.get("department") or "Unknown", "department": r.get("department") or "", "allotted": float(r.get("allotted") or 0), "consumed": float(r.get("consumed") or 0)} for r in rows]

    return JsonResponse({"items": items})


# ---------------------------------------------------------------------
#  Existing compatibility endpoints left unchanged (pdl_dept_summary, pdl_cost_series etc.)
#  We did not remove or modify them to preserve existing callers.
# ---------------------------------------------------------------------
def pdl_dept_summary(request, year):
    """Department summary restricted to logged-in user's created IOMs."""
    creator_cn = request.session.get("cn") or request.session['ldap_username'] or ""
    sql = """
        SELECT department, SUM(total_hours) AS hours
        FROM prism_wbs
        WHERE year = %s AND creator = %s
        GROUP BY department
    """
    rows = dict_fetchall(sql, (str(year), creator_cn))
    labels = [r["department"] or "Unknown" for r in rows]
    values = [float(r["hours"] or 0) for r in rows]
    return JsonResponse({"labels": labels, "data": values})
