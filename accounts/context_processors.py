# context_processors.py
from copy import deepcopy
from django.urls import reverse_lazy


# -------------------------
# MENU TREE (with icons)
# -------------------------
# Each item may include:
# - key: unique key
# - title: visible text
# - url: url or reverse_lazy(...) call
# - icon: Font Awesome icon name (no "fa-" prefix, e.g. "tachometer-alt")
# - roles: list of role strings allowed to see the item
# - submenus: optional list of submenu dicts with same fields
#
# Adjust the `roles` values to match your application's role names.
MENU_TREE = [
    {
        "key": "dashboard",
        "title": "Dashboard",
        "icon": "gauge-high",  # or "tachometer-alt" if using FA5
        "url": reverse_lazy("dashboard:home"),
        "roles": ["ADMIN", "PDL", "TEAM_LEAD", "COE_LEADER", "EMPLOYEE"],
    },

    {
        "key": "projects",
        "title": "Projects",
        "icon": "project-diagram",
        "url": reverse_lazy("projects:list"),
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
        "submenus": [
            {
                "key": "projects_list",
                "title": "Projects List",
                "icon": "list-alt",
                "url": reverse_lazy("projects:list"),
                "roles": ["ADMIN", "PDL"],
            },
            {
                "key": "create_project",
                "title": "Edit Project",
                "icon": "edit",
                "url": reverse_lazy("projects:edit_default"),
                "roles": ["ADMIN", "PDL"],
            },
        ],
    },
    {
        "key": "resources",
        "title": "Resource Management",
        "icon": "users-cog",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
        "submenus": [
            {
                "key": "directory",
                "title": "Employee Directory",
                "icon": "address-book",
                "url": reverse_lazy("resources:directory"),
                "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
            },
            {
                "key": "ldap_sync",
                "title": "Import / Sync LDAP",
                "icon": "sync-alt",
                "url": reverse_lazy("resources:ldap_sync"),
                "roles": ["ADMIN", "PDL"],
            },
        ],
    },
    {
        "key": "allocations",
        "title": "Allocations",
        "icon": "calendar-alt",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD", "EMPLOYEE"],
        "submenus": [
            {
                "key": "monthly",
                "title": "Monthly Allocation",
                "icon": "calendar",
                "url": reverse_lazy("projects:monthly_allocations"),
                "roles": ["PDL", "ADMIN"],
            },
            {
                "key": "weekly",
                "title": "Team Allocation",
                "icon": "users",
                "url": reverse_lazy("projects:tl_allocations"),
                "roles": ["COE_LEADER", "TEAM_LEAD", "ADMIN", "PDL"],
            },
            {
                "key": "my_alloc",
                "title": "My Allocations",
                "icon": "user-clock",
                "url": reverse_lazy("projects:my_allocations"),
                "roles": ["COE_LEADER", "TEAM_LEAD", "ADMIN", "PDL", "EMPLOYEE"],
            },
        ],
    },
    {
        "key": "coes",
        "title": "COE & Domains",
        "icon": "sitemap",
        "url": "#",
        "roles": ["ADMIN", "COE_LEADER", "TEAM_LEAD"],
        "submenus": [
            {
                "key": "coe_list",
                "title": "COE List",
                "icon": "th-list",
                "url": reverse_lazy("coes:list") if False else "#",  # replace if you have a view
                "roles": ["ADMIN", "COE_LEADER"],
            },
            {
                "key": "add_coe",
                "title": "Add / Edit COE",
                "icon": "plus-square",
                "url": "#",
                "roles": ["ADMIN"],
            },
        ],
    },
    {
        "key": "reports",
        "title": "Reports & Analytics",
        "icon": "chart-line",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
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
                "icon": "file-alt",
                "url": "#",
                "roles": ["ADMIN"],
            },
        ],
    },
    {
        "key": "settings",
        "title": "Settings",
        "icon": "cog",
        "url": "#",
        "roles": ["ADMIN", "PDL"],
        "submenus": [
            {
                "key": "import_master",
                "title": "Import IOM Master",
                "icon": "upload",
                "url": reverse_lazy("settings:import_master"),
                "roles": ["ADMIN", "PDL"],
            },
            {
                "key": "import_fce_projects_master",
                "title": "Import FCE Projects",
                "icon": "upload",
                "url": reverse_lazy("settings:import_fce_projects"),
                "roles": ["ADMIN", "PDL"],
            },
            {
                "key": "monthly_hours",
                "title": "Monthly hours limit",
                "icon": "clock",
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
                "icon": "tools",
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
    Modify this function to match how you represent roles in your project.
    It attempts several fallbacks:
      1. request.user.groups (Django auth groups)
      2. request.session['roles'] (custom session)
      3. request.session['role'] (single role string)
      4. request.user.is_superuser -> ADMIN
    """

    roles = set()

    user = getattr(request, "user", None)
    if user and user.is_authenticated:
        # prefer Django groups if used
        try:
            groups = user.groups.all()
            for g in groups:
                roles.add(str(g.name).upper())
        except Exception:
            pass

        # superuser -> ADMIN
        if getattr(user, "is_superuser", False):
            roles.add("ADMIN")

    # fallback: session might store roles
    sess = getattr(request, "session", None)
    if sess:
        # if session contains list of roles
        rlist = sess.get("roles") or sess.get("user_roles")
        if isinstance(rlist, (list, tuple)):
            for r in rlist:
                roles.add(str(r).upper())
        else:
            # single role string
            r = sess.get("role") or sess.get("user_role")
            if r:
                roles.add(str(r).upper())

    # final fallback: if no roles found, return EMPLOYEE to show basic menus
    if not roles:
        roles.add("EMPLOYEE")

    return roles


def _filter_menu_by_roles(menu_tree, roles):
    """
    Deep copy the menu_tree and remove items/submenus the user is not allowed to see.
    Returns a filtered copy which is safe to pass to templates.
    """
    filtered = []
    for item in menu_tree:
        item_roles = set([r.upper() for r in item.get("roles", [])]) if item.get("roles") else set()
        # if item has no roles specified, assume visible to all
        visible_item = False
        if not item_roles:
            visible_item = True
        elif roles & item_roles:
            visible_item = True

        if not visible_item:
            continue

        # shallow copy the item so we can modify submenus
        item_copy = deepcopy(item)
        submenus = item_copy.get("submenus")
        if submenus:
            filtered_subs = []
            for s in submenus:
                s_roles = set([r.upper() for r in s.get("roles", [])]) if s.get("roles") else set()
                # if no roles on submenu -> visible
                if not s_roles or (roles & s_roles):
                    filtered_subs.append(deepcopy(s))
            # only include submenus key if any left
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
    Use this in settings.TEMPLATES['OPTIONS']['context_processors'].
    """
    try:
        roles = _get_user_roles(request)
        filtered = _filter_menu_by_roles(MENU_TREE, roles)
    except Exception:
        # in case anything fails, return the safe default (empty menu)
        filtered = []

    return {
        "feas_menu": filtered,
    }
# at top or bottom of accounts/context_processors.py
# if you already have feas_menu(request) defined, add:

def menu_processor(request):
    """
    Backwards-compatible wrapper. Existing settings can keep referencing
    'accounts.context_processors.menu_processor'.
    """
    return feas_menu(request)
