# dashboard/views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.db import connection
from datetime import datetime

def dict_fetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def tl_dashboard(request):
    if not request.session.get('is_authenticated') :
        return render(request, "403.html")
    return render(request, "dashboard/tl_dashboard.html")

def tl_dashboard_filters(request):
    user_ldap = request.session['ldap_username']
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT YEAR(month_start) as y FROM team_distributions WHERE lead_ldap=%s ORDER BY y", [user_ldap])
        years = [r['y'] for r in dict_fetchall(cursor)]
        if not years:
            years = [datetime.now().year]
        months = [{'value': i, 'label': datetime(2000, i, 1).strftime('%b')} for i in range(1, 13)]
        cursor.execute("""
            SELECT DISTINCT p.name FROM projects p
            JOIN team_distributions td ON td.project_id = p.id
            WHERE td.lead_ldap=%s
            ORDER BY p.name
        """, [user_ldap])
        programs = [r['name'] for r in dict_fetchall(cursor)]
    return JsonResponse({'years': years, 'months': months, 'programs': programs})

def tl_dashboard_data(request):
    user_ldap = request.session['ldap_username']
    year = int(request.GET.get('year', datetime.now().year))
    month = int(request.GET.get('month', datetime.now().month))
    program = request.GET.get('program', '')

    params = [user_ldap, f"{year}-{month:02d}-01"]
    program_filter = ""
    if program:
        program_filter = "AND p.name = %s"
        params.append(program)

    # 1. Team Capacity vs Actual Load
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(DISTINCT td.reportee_ldap) as team_capacity
            FROM team_distributions td
            JOIN projects p ON td.project_id = p.id
            WHERE td.lead_ldap=%s AND td.month_start=%s {program_filter}
        """, params)
        team_capacity = cursor.fetchone()[0] or 0

        cursor.execute(f"""
            SELECT COUNT(DISTINCT td.reportee_ldap) as team_actual
            FROM team_distributions td
            JOIN punch_data pd ON pd.team_distribution_id = td.id
            JOIN projects p ON td.project_id = p.id
            WHERE td.lead_ldap=%s AND td.month_start=%s AND pd.punched_hours > 0 {program_filter}
        """, params)
        team_actual = cursor.fetchone()[0] or 0

        # 2. Reportee vs Utilization
        cursor.execute(f"""
            SELECT u.cn as name,
                SUM(td.hours)/183.75 as planned_fte,
                SUM(pd.punched_hours)/183.75 as actual_fte
            FROM team_distributions td
            JOIN ldap_directory u ON u.username = td.reportee_ldap
            LEFT JOIN punch_data pd ON pd.team_distribution_id = td.id
            JOIN projects p ON td.project_id = p.id
            WHERE td.lead_ldap=%s AND td.month_start=%s {program_filter}
            GROUP BY u.cn
        """, params)
        reportees = dict_fetchall(cursor)

        # 3. Deviation: Planned vs Consumed Hours (line graph)
        cursor.execute(f"""
            SELECT DATE_FORMAT(td.month_start, '%%b %%Y') as label,
                SUM(td.hours) as planned,
                SUM(pd.punched_hours) as consumed
            FROM team_distributions td
            LEFT JOIN punch_data pd ON pd.team_distribution_id = td.id
            JOIN projects p ON td.project_id = p.id
            WHERE td.lead_ldap=%s {program_filter}
            GROUP BY td.month_start
            ORDER BY td.month_start
        """, [user_ldap] + ([program] if program else []))
        deviation = dict_fetchall(cursor)
        deviation_labels = [r['label'] for r in deviation]
        deviation_planned = [float(r['planned'] or 0) for r in deviation]
        deviation_consumed = [float(r['consumed'] or 0) for r in deviation]

        # 4. FTE Utilization by Project/Subproject
        cursor.execute(f"""
            SELECT CONCAT(p.name, '/', sp.name) as label,
                SUM(td.hours)/183.75 as fte
            FROM team_distributions td
            JOIN projects p ON td.project_id = p.id
            JOIN subprojects sp ON td.subproject_id = sp.id
            WHERE td.lead_ldap=%s AND td.month_start=%s {program_filter}
            GROUP BY p.name, sp.name
            ORDER BY fte DESC
        """, params)
        fte_projects = dict_fetchall(cursor)
        fte_projects_labels = [r['label'] for r in fte_projects]
        fte_projects_fte = [float(r['fte'] or 0) for r in fte_projects]

        # 5. Self Allocation (FTE by Program)
        cursor.execute(f"""
            SELECT p.name as label,
                SUM(pd.punched_hours)/183.75 as fte
            FROM punch_data pd
            JOIN projects p ON pd.project_id = p.id
            WHERE pd.user_email=%s AND YEAR(pd.month_start)=%s
            GROUP BY p.name
            ORDER BY fte DESC
        """, [user_ldap, year])
        self_alloc = dict_fetchall(cursor)
        self_alloc_labels = [r['label'] for r in self_alloc]
        self_alloc_fte = [float(r['fte'] or 0) for r in self_alloc]

    return JsonResponse({
        'team_capacity': team_capacity,
        'team_actual': team_actual,
        'reportees': reportees,
        'deviation': {
            'labels': deviation_labels,
            'planned': deviation_planned,
            'consumed': deviation_consumed,
        },
        'fte_projects': {
            'labels': fte_projects_labels,
            'fte': fte_projects_fte,
        },
        'self_alloc': {
            'labels': self_alloc_labels,
            'fte': self_alloc_fte,
        }
    })