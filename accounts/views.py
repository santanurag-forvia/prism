# accounts/views.py
from django.shortcuts import render, redirect
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from django.conf import settings
from django.urls import reverse
import threading
import logging
import datetime

# LDAP imports (same approach as your reference)
from ldap3 import Server, Connection, ALL, SUBTREE
# reuse the same check_credentials and build_bind_username logic (adapted below)
# Also import your initializer
from feas_project.db_initializer import initialize_database
from .ldap_utils import get_user_entry_by_username, get_reportees_for_user_dn
import logging
import datetime
import threading
from urllib.parse import urlencode

from django.conf import settings
from django.shortcuts import render, redirect
from django.urls import reverse
from django.http import HttpResponseRedirect
from django.contrib import messages
from django.utils.http import url_has_allowed_host_and_scheme

logger = logging.getLogger(__name__)

def _get_logged_in_username_from_session(request):
    # adapt to how you saved session in login_view earlier:
    # earlier we set request.session['ldap_username'] = username (likely sAMAccountName or UPN)
    return request.session.get('username')

def reportees_view(request):
    """
    Public view (or guarded by login) showing reportees of currently logged-in user.
    """
    username = _get_logged_in_username_from_session(request)
    if not username:
        messages.error(request, "Could not determine current user. Please login.")
        return redirect('accounts:login')

    # 1. resolve user entry (to get DN)
    user_entry = get_user_entry_by_username(username)
    if not user_entry:
        messages.error(request, "User not found in LDAP.")
        return redirect('accounts:login')

    user_dn = user_entry.entry_dn
    # 2. fetch reportees
    reportees = get_reportees_for_user_dn(user_dn)

    # 3. Render template
    return render(request, "accounts/reportees.html", {
        "manager_cn": str(user_entry.cn) if hasattr(user_entry, 'cn') else username,
        "reportees": reportees,
    })

def build_bind_username(input_username):
    if '@' in input_username:
        search_filter = f'(userPrincipalName={input_username})'
        return input_username, search_filter
    else:
        domain = getattr(settings, "LDAP_DOMAIN_PREFIX", "LS")  # optional override
        user_input = input_username
        login_username = f"{domain}\\{user_input}"
        search_filter = f'(sAMAccountName={input_username})'
        return login_username, search_filter

def check_credentials_bind(username: str, password: str):
    """
    Attempt to bind with provided username/password.
    Returns (is_authenticated: bool, conn_or_none: ldap3.Connection or None, user_entry or None, error_message or None)
    - conn is a bound ldap3.Connection using the user's credentials (caller must unbind() when done)
    - user_entry is the ldap entry of the user (if found)
    """
    AD_SERVER = getattr(settings, "LDAP_SERVER", None)
    AD_PORT = int(getattr(settings, "LDAP_PORT", 389))
    USER_SEARCH_BASE = getattr(settings, "LDAP_USER_SEARCH_BASE", None)
    BASE_DN = getattr(settings, "LDAP_BASE_DN", None)

    if not AD_SERVER or not BASE_DN:
        return False, None, None, "LDAP server or base DN not configured."

    bind_user, search_filter = build_bind_username(username)
    logger.debug("Attempting to bind as %s", bind_user)

    server = Server(AD_SERVER, port=AD_PORT, get_info=ALL)
    try:
        conn = Connection(server, user=bind_user, password=password, receive_timeout=10)
        if not conn.bind():
            logger.debug("Bind failed for user %s", username)
            # ensure closed
            try:
                conn.unbind()
            except Exception:
                pass
            return False, None, None, None

        # bound successfully. Now search the user entry to fetch attributes (reuse the same connection)
        search_base = f"{USER_SEARCH_BASE},{BASE_DN}" if USER_SEARCH_BASE else BASE_DN
        attributes = getattr(settings, "LDAP_ATTRIBUTES", [
            'cn', 'sAMAccountName', 'userPrincipalName', 'mail', 'department',
            'title', 'telephoneNumber', 'lastLogonTimestamp', 'memberOf', 'jpegPhoto', 'manager', 'directReports'
        ])
        conn.search(search_base=search_base, search_filter=search_filter, search_scope=SUBTREE, attributes=attributes)
        user_entry = conn.entries[0] if conn.entries else None

        return True, conn, user_entry, None
    except Exception as e:
        logger.exception("LDAP bind/search error: %s", e)
        return False, None, None, "LDAP connection error"


# accounts/views.py


# Import your LDAP helpers
# from .ldap_helpers import check_credentials_bind, get_user_entry_by_username, get_reportees_for_user_dn, map_role_from_ldap_attrs
# from .initializers import initialize_database

from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme

@csrf_exempt
def login_view(request):
    print("[DEBUG] Entered login_view")
    if request.session.get('is_authenticated'):
        print("[DEBUG] User already authenticated, redirecting to dashboard:home")
        return redirect('dashboard:home') if 'dashboard' in settings.INSTALLED_APPS else redirect('/')

    print(f"[DEBUG] Request method: {request.method}")
    if request.method == "POST":
        username = request.POST.get("username", "").strip()
        password = request.POST.get("password", "")
        print(f"[DEBUG] POST received. Username: '{username}', Password provided: {'Yes' if password else 'No'}")

        if not username or not password:
            print("[DEBUG] Username or password missing")
            messages.error(request, "Please enter username and password.")
            return render(request, "accounts/login.html")

        # Superadmin path (unchanged)
        if username == getattr(settings, "FEAS_SUPERADMIN_USERNAME", "admin") and password == getattr(settings, "FEAS_SUPERADMIN_PASSWORD", "admin"):
            print("[DEBUG] Superadmin login detected")
            request.session['is_authenticated'] = True
            request.session['ldap_username'] = username
            request.session['cn'] = 'Administrator'
            request.session['title'] = 'Administrator'
            request.session['role'] = "ADMIN"
            request.session['last_login'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Save session immediately so cookie is set before redirect
            try:
                request.session.save()
                print("[DEBUG] Superadmin session saved")
            except Exception as e:
                print(f"[DEBUG] Error saving session for superadmin: {e}")
            return HttpResponseRedirect(reverse('dashboard:home'))

        # LDAP bind/auth
        is_auth, conn, user_entry, err = check_credentials_bind(username, password)
        print(f"[DEBUG] LDAP bind result: is_auth={is_auth}, err={err}")

        if err:
            print(f"[DEBUG] LDAP error: {err}")
            messages.error(request, err)
            return render(request, "accounts/login.html")

        if is_auth and conn:
            print("[DEBUG] LDAP authentication successful")
            # set session values
            request.session['is_authenticated'] = True
            request.session['ldap_username'] = username
            request.session['ldap_password'] = password
            request.session['cn'] = str(getattr(user_entry, 'cn', username)) if user_entry else username
            request.session['title'] = str(getattr(user_entry, 'title', '')) if user_entry else ''
            request.session['last_login'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # map role safely
            try:
                user_details = {}
                if user_entry:
                    user_details['department'] = getattr(user_entry, 'department', None).value if hasattr(user_entry, 'department') and getattr(user_entry, 'department', None) else ""
                    user_details['title'] = getattr(user_entry, 'title', None).value if hasattr(user_entry, 'title') and getattr(user_entry, 'title', None) else ""
                role = map_role_from_ldap_attrs(user_entry, user_details)
                print("Role mapped to:", role)
                request.session['role'] = role or "EMPLOYEE"
            except Exception as e:
                print(f"[DEBUG] Role mapping failed: {e}")
                request.session['role'] = "EMPLOYEE"

            # close LDAP connection
            try:
                conn.unbind()
            except Exception:
                pass

            # ===== IMPORTANT: save session BEFORE launching DB init or redirect =====
            try:
                request.session.save()
                initialize_database();  # ensure DB init is done at least once
                print("[DEBUG] Session saved successfully after LDAP login. Session keys:", dict(request.session.items()))
            except Exception as e:
                print(f"[DEBUG] Error saving session after LDAP login: {e}")


            # Safe redirect: prefer validated next param, else dashboard home
            next_url = request.POST.get('next') or request.GET.get('next')
            if next_url and url_has_allowed_host_and_scheme(next_url, {request.get_host()}, require_https=request.is_secure()):
                print("[DEBUG] Redirecting to validated next_url:", next_url)
                return HttpResponseRedirect(next_url)

            home_url = reverse('dashboard:home')
            print("[DEBUG] Redirecting to dashboard home:", home_url)
            return HttpResponseRedirect(home_url)

        # auth failed
        messages.error(request, "Invalid username or password.")
        return render(request, "accounts/login.html")

    # GET
    return render(request, "accounts/login.html")


def logout_view(request):
    """
    Clear session and redirect to login.
    """
    # Remove keys we added (safe) and flush the session
    try:
        request.session.flush()   # this removes all session data and creates a new empty session
    except Exception:
        # fallback: pop known keys
        for k in ['is_authenticated', 'username', 'ldap_username', 'ldap_dn', 'cn', 'title', 'role', 'last_login']:
            request.session.pop(k, None)

    messages.success(request, "Logged out.")
    return redirect('accounts:login')

# accounts/views.py (add or paste near the imports)
def map_role_from_ldap_attrs(user_entry, user_details):
    """
    Return canonical role string based on LDAP entry attributes or user_details.
    Strategy:
      - If user_entry has memberOf (groups), try to match common role keywords.
      - Else fallback to department/title heuristics in user_details.
    """
    # prioritize memberOf groups
    member_of = None
    print("[DEBUG] Mapping role from LDAP attributes")
    if user_entry is not None and hasattr(user_entry, 'memberOf') and user_entry.memberOf:
        try:
            # ldap3 multi-valued attr may be in .memberOf.values or iterable
            member_of = list(user_entry.memberOf.values) if hasattr(user_entry.memberOf, 'values') else list(user_entry.memberOf)
        except Exception:
            member_of = None

    if member_of:
        # Normalize strings and detect keywords
        for grp in member_of:
            g = str(grp).lower()
            if "admin" in g or "admins" in g:
                return "ADMIN"
            if "pdl" in g or "program" in g or "tpl" in g or "project" in g:
                return "PDL"
            if "coe" in g or "centerofexcellence" in g or "coe_leader" in g:
                return "COE_LEADER"
            if "team" in g and "lead" in g:
                return "TEAM_LEAD"

    # fallback heuristic using department/title
    dept = user_details.get("department", "") if user_details else ""
    title = user_details.get("title", "") if user_details else ""
    if dept and "engineering" in dept.lower() and "lead" in str(title).lower():
        return "TEAM_LEAD"
    if "manager" in str(title).lower() or "lead" in str(title).lower():
        return "TEAM_LEAD"
    # default
    return "EMPLOYEE"
