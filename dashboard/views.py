from django.shortcuts import render
from django.http import JsonResponse
from django.db import connection
from datetime import date
from collections import defaultdict

def dict_fetchall(sql, params=None):
    with connection.cursor() as c:
        c.execute(sql, params or [])
        columns = [col[0] for col in c.description]
        return [dict(zip(columns, row)) for row in c.fetchall()]

def tl_dashboard(request):
    print("TL Dashboard accessed by:", request.session.get('ldap_username'))
    print("Session data:", request.session.items())
    if not request.session.get('is_authenticated') :
        return render(request, "403.html")
    return render(request, "dashboard/tl_dashboard.html")

def tl_dashboard_filters(request):
    # Years from punch_data, months as 1-12, programs from projects
    years = dict_fetchall("SELECT DISTINCT YEAR(punch_date) as y FROM punch_data ORDER BY y")
    months = [{'value': str(i), 'label': date(2000, i, 1).strftime('%b')} for i in range(1, 13)]
    programs = dict_fetchall("SELECT DISTINCT name FROM projects ORDER BY name")
    return JsonResponse({
        'years': [y['y'] for y in years],
        'months': months,
        'programs': [p['name'] for p in programs]
    })

def tl_dashboard_data(request):
    user_email = request.session['ldap_username']
    year = int(request.GET.get('year', date.today().year))
    month = int(request.GET.get('month', date.today().month))
    program = request.GET.get('program', '')

    # 1. Team Capacity vs Actual Load
    # Capacity: count of reportees, Actual: count with nonzero FTE
    sql_reportees = """
        SELECT DISTINCT td.reportee_ldap as email
        FROM team_distributions td
        WHERE td.lead_ldap=%s AND YEAR(td.month_start)=%s AND MONTH(td.month_start)=%s
    """
    reportees = dict_fetchall(sql_reportees, [user_email, year, month])
    team_capacity = len(reportees)
    sql_actual = """
        SELECT COUNT(DISTINCT pd.user_email) as cnt
        FROM punch_data pd
        JOIN team_distributions td ON pd.team_distribution_id=td.id
        WHERE td.lead_ldap=%s AND YEAR(pd.punch_date)=%s AND MONTH(pd.punch_date)=%s
          AND pd.punched_hours > 0
    """
    team_actual = dict_fetchall(sql_actual, [user_email, year, month])[0]['cnt']

    # 2. Reportee vs Utilization
    sql_rep_util = """
        SELECT td.reportee_ldap as name,
            SUM(td.hours)/183.75 as planned_fte,
            SUM(pd.punched_hours)/183.75 as actual_fte
        FROM team_distributions td
        LEFT JOIN punch_data pd ON pd.team_distribution_id=td.id
            AND YEAR(pd.punch_date)=%s AND MONTH(pd.punch_date)=%s
        WHERE td.lead_ldap=%s
        GROUP BY td.reportee_ldap
    """
    rep_util = dict_fetchall(sql_rep_util, [year, month, user_email])

    # 3. Deviation: Planned vs Consumed Hours (by week)
    sql_weeks = """
        SELECT WEEK(pd.punch_date, 1) as week, 
            SUM(td.hours) as planned, 
            SUM(pd.punched_hours) as consumed
        FROM team_distributions td
        LEFT JOIN punch_data pd ON pd.team_distribution_id=td.id
            AND YEAR(pd.punch_date)=%s AND MONTH(pd.punch_date)=%s
        WHERE td.lead_ldap=%s
        GROUP BY week
        ORDER BY week
    """
    weeks = dict_fetchall(sql_weeks, [year, month, user_email])
    deviation = {
        'labels': [f"W{w['week']}" for w in weeks],
        'planned': [float(w['planned'] or 0) for w in weeks],
        'consumed': [float(w['consumed'] or 0) for w in weeks]
    }

    # 4. FTE Utilization by Project/Subproject
    sql_proj = """
        SELECT p.name as label, SUM(pd.punched_hours)/183.75 as fte
        FROM punch_data pd
        JOIN projects p ON pd.project_id=p.id
        JOIN team_distributions td ON pd.team_distribution_id=td.id
        WHERE td.lead_ldap=%s AND YEAR(pd.punch_date)=%s AND MONTH(pd.punch_date)=%s
        GROUP BY p.name
        ORDER BY fte DESC
    """
    fte_projects = dict_fetchall(sql_proj, [user_email, year, month])
    fte_projects_data = {
        'labels': [p['label'] for p in fte_projects],
        'fte': [float(p['fte'] or 0) for p in fte_projects]
    }

    # 5. Self Allocation (FTE by Program for logged-in user)
    sql_self = """
        SELECT p.name as label, SUM(pd.punched_hours)/183.75 as fte
        FROM punch_data pd
        JOIN projects p ON pd.project_id=p.id
        WHERE pd.user_email=%s AND YEAR(pd.punch_date)=%s AND MONTH(pd.punch_date)=%s
        GROUP BY p.name
        ORDER BY fte DESC
    """
    self_alloc = dict_fetchall(sql_self, [user_email, year, month])
    self_alloc_data = {
        'labels': [s['label'] for s in self_alloc],
        'fte': [float(s['fte'] or 0) for s in self_alloc]
    }

    return JsonResponse({
        'team_capacity': team_capacity,
        'team_actual': team_actual,
        'reportees': [
            {
                'name': r['name'],
                'planned_fte': float(r['planned_fte'] or 0),
                'actual_fte': float(r['actual_fte'] or 0)
            } for r in rep_util
        ],
        'deviation': deviation,
        'fte_projects': fte_projects_data,
        'self_alloc': self_alloc_data
    })