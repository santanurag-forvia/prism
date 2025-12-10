# context_processors.py
from copy import deepcopy
from django.urls import reverse_lazy


# -------------------------
# MENU TREE (with icons)
# -------------------------
MENU_TREE = [
    {
        "key": "dashboard",
        "title": "Dashboard",
        "icon": "chart-line",
        "url": reverse_lazy("dashboard:tl_dashboard"),
        "roles": ["ADMIN", "PDL", "TEAM_LEAD", "COE_LEADER", "EMPLOYEE"],
    },

    # {
    #     "key": "projects",
    #     "title": "Projects",
    #     "icon": "diagram-project",
    #     "url": reverse_lazy("projects:list"),
    #     "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
    #     "submenus": [
    #         {
    #             "key": "projects_list",
    #             "title": "Projects List",
    #             "icon": "list",
    #             "url": reverse_lazy("projects:list"),
    #             "roles": ["ADMIN", "PDL"],
    #         },
    #         {
    #             "key": "create_project",
    #             "title": "Edit Project",
    #             "icon": "pen-to-square",
    #             "url": reverse_lazy("projects:edit_default"),
    #             "roles": ["ADMIN", "PDL"],
    #         },
    #     ],
    # },
    {
        "key": "resources",
        "title": "Resource Management",
        "icon": "users-gear",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER"],
        "submenus": [
            {
                "key": "directory",
                "title": "Employee Directory",
                "icon": "address-book",
                "url": reverse_lazy("resources:directory"),
                "roles": ["ADMIN", "PDL", "COE_LEADER"],
            },
            {
                "key": "ldap_sync",
                "title": "Import / Sync LDAP",
                "icon": "arrows-rotate",
                "url": reverse_lazy("resources:ldap_sync"),
                "roles": ["ADMIN", "PDL"],
            },
        ],
    },
    {
        "key": "allocations",
        "title": "Allocations",
        "icon": "calendar-days",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD", "EMPLOYEE"],
        "submenus": [
            {
                "key": "monthly",
                "title": "Monthly Allocation",
                "icon": "calendar-check",
                "url": reverse_lazy("projects:monthly_allocations"),
                "roles": ["PDL", "ADMIN"],
            },
            {
                "key": "weekly",
                "title": "Team Allocation",
                "icon": "users",  # FIXED
                "url": reverse_lazy("projects:tl_allocations"),
                "roles": ["COE_LEADER", "TEAM_LEAD", "ADMIN", "PDL"],
            },
            {
                "key": "my_alloc",
                "title": "My Allocations",
                "icon": "clock",
                "url": reverse_lazy("projects:my_allocations"),
                "roles": ["COE_LEADER", "TEAM_LEAD", "ADMIN", "PDL", "EMPLOYEE"],
            },
            {
                "key": "tl_punch_review",
                "title": "TL Punching Review",
                "icon": "clock",
                "url": reverse_lazy("projects:tl_punch_review"),
                "roles": ["COE_LEADER", "TEAM_LEAD", "ADMIN", "PDL", "EMPLOYEE"],
            },
        ],
    },
    {
        "key": "coes",
        "title": "COE & Domains",
        "icon": "sitemap",
        "url": "#",
        "roles": ["ADMIN", "COE_LEADER"],
        "submenus": [
            {
                "key": "coe_list",
                "title": "COE List",
                "icon": "rectangle-list",
                "url": reverse_lazy("coes:list") if False else "#",
                "roles": ["ADMIN", "COE_LEADER"],
            },
            {
                "key": "add_coe",
                "title": "Add / Edit COE",
                "icon": "square-plus",
                "url": "#",
                "roles": ["ADMIN"],
            },
        ],
    },
    {
        "key": "reports",
        "title": "Reports & Analytics",
        "icon": "chart-bar",
        "url": "#",
        "roles": ["ADMIN"],
        "submenus": [
            {
                "key": "util",
                "title": "Utilization",
                "icon": "chart-pie",
                "url": "#",
                "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
            },
            {
                "key": "custom",
                "title": "Custom Report",
                "icon": "file-lines",
                "url": "#",
                "roles": ["ADMIN"],
            },
        ],
    },
    {
        "key": "settings",
        "title": "Settings",
        "icon": "gear",
        "url": "#",
        "roles": ["ADMIN", "PDL"],
        "submenus": [
            {
                "key": "import_master",
                "title": "Import IOM Master",
                "icon": "file-import",
                "url": reverse_lazy("settings:import_master"),
                "roles": ["ADMIN", "PDL"],
            },
            {
                "key": "import_fce_projects_master",
                "title": "Import FCE Projects",
                "icon": "cloud-arrow-up",
                "url": reverse_lazy("settings:import_fce_projects"),
                "roles": ["ADMIN", "PDL"],
            },
            {
                "key": "monthly_hours",
                "title": "Monthly hours limit",
                "icon": "hourglass-half",
                "url": reverse_lazy("settings:monthly_hours_settings"),
                "roles": ["PDL", "ADMIN"],
            },
            {
                "key": "holidays",
                "title": "Annual Holidays",
                "icon": "umbrella-beach",
                "url": reverse_lazy("settings:settings_holidays"),
                "roles": ["PDL", "ADMIN"],
            },
            {
                "key": "ldap",
                "title": "LDAP Configuration",
                "icon": "server",
                "url": "#",
                "roles": ["ADMIN", "PDL"],
            },
            {
                "key": "system",
                "title": "System Config",
                "icon": "sliders",
                "url": "#",
                "roles": ["ADMIN", "PDL"],
            },
        ],
    },
    {
        "key": "admin",
        "title": "Admin",
        "icon": "user-shield",
        "url": "/admin/",
        "roles": ["ADMIN"],
    },
]


# -------------------------
# Helpers
# -------------------------
def _get_user_roles(request):
    """
    Return a list of role strings for the logged-in user.
    """
    print("[_get_user_roles] Called")
    roles = set()
    user = getattr(request, "user", None)
    print(f"[_get_user_roles] user: {user}")

    if user and user.is_authenticated:
        print("[_get_user_roles] User is authenticated")
        try:
            groups = user.groups.all()
            print(f"[_get_user_roles] User groups: {groups}")
            for g in groups:
                print(f"[_get_user_roles] Adding group role: {g.name}")
                roles.add(str(g.name).upper())
        except Exception as e:
            print(f"[_get_user_roles] Exception in groups: {e}")

        if getattr(user, "is_superuser", False):
            print("[_get_user_roles] User is superuser, adding ADMIN")
            roles.add("ADMIN")

    sess = getattr(request, "session", None)
    print(f"[_get_user_roles] session: {sess}")
    if sess:
        rlist = sess.get("roles") or sess.get("user_roles")
        print(f"[_get_user_roles] session roles list: {rlist}")
        if isinstance(rlist, (list, tuple)):
            for r in rlist:
                print(f"[_get_user_roles] Adding session role from list: {r}")
                roles.add(str(r).upper())
        else:
            r = sess.get("role") or sess.get("user_role")
            print(f"[_get_user_roles] session single role: {r}")
            if r:
                roles.add(str(r).upper())

    if not roles:
        print("[_get_user_roles] No roles found, defaulting to EMPLOYEE")
        roles.add("EMPLOYEE")

    print(f"[_get_user_roles] Final roles: {roles}")
    return roles

def _filter_menu_by_roles(menu_tree, roles):
    """
    Deep copy the menu_tree and remove items/submenus the user is not allowed to see.
    """
    filtered = []
    for item in menu_tree:
        item_roles = set([r.upper() for r in item.get("roles", [])]) if item.get("roles") else set()
        visible_item = False
        if not item_roles:
            visible_item = True
        elif roles & item_roles:
            visible_item = True

        if not visible_item:
            continue

        item_copy = deepcopy(item)
        submenus = item_copy.get("submenus")
        if submenus:
            filtered_subs = []
            for s in submenus:
                s_roles = set([r.upper() for r in s.get("roles", [])]) if s.get("roles") else set()
                if not s_roles or (roles & s_roles):
                    filtered_subs.append(deepcopy(s))
            if filtered_subs:
                item_copy["submenus"] = filtered_subs
            else:
                item_copy.pop("submenus", None)
        filtered.append(item_copy)

    return filtered


# -------------------------
# Context processor
# -------------------------
def feas_menu(request):
    """
    Context processor to inject the filtered menu into templates as 'feas_menu'.
    """
    try:
        roles = _get_user_roles(request)
        filtered = _filter_menu_by_roles(MENU_TREE, roles)
    except Exception:
        filtered = []

    return {
        "feas_menu": filtered,
    }


def menu_processor(request):
    """
    Backwards-compatible wrapper.
    """
    return feas_menu(request)