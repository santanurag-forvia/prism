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
            try:
                conn.unbind()
            except Exception:
                pass
            return False, None, None, None

        search_base = f"{USER_SEARCH_BASE},{BASE_DN}" if USER_SEARCH_BASE else BASE_DN
        attributes = getattr(settings, "LDAP_ATTRIBUTES", [
            'cn', 'sAMAccountName', 'userPrincipalName', 'mail', 'department',
            'title', 'telephoneNumber', 'lastLogonTimestamp', 'memberOf', 'jpegPhoto',
            'manager', 'directReports',
            'physicalDeliveryOfficeName', 'l', 'st', 'c', 'co', 'postalCode', 'streetAddress'
        ])

        conn.search(search_base=search_base, search_filter=search_filter, search_scope=SUBTREE, attributes=['*'])
        user_entry = conn.entries[0] if conn.entries else None

        if user_entry:
            print("\n--- Location Details ---")
            location_info = {
                "City": user_entry["l"].value if "l" in user_entry else None,
                "Office": user_entry[
                    "physicalDeliveryOfficeName"].value if "physicalDeliveryOfficeName" in user_entry else None,
                "Street": user_entry["streetAddress"].value if "streetAddress" in user_entry else None,
                "Postal Code": user_entry["postalCode"].value if "postalCode" in user_entry else None,
                "Country": user_entry["co"].value if "co" in user_entry else None,
                "Country Code": user_entry["c"].value if "c" in user_entry else None,
                "Usage Location": user_entry[
                    "msExchUsageLocation"].value if "msExchUsageLocation" in user_entry else None,
                "Site Code": user_entry["extensionAttribute5"].value if "extensionAttribute5" in user_entry else None
            }
            # for key, val in location_info.items():
            #     print(f"{key}: {val}")

        return True, conn, user_entry, None

    except Exception as e:
        logger.exception("LDAP bind/search error: %s", e)
        return False, None, None, "LDAP connection error"


EU_COUNTRY_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR",
    "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK",
    "SI", "ES", "SE"
}

def is_eu_country(country_code):
    """
    Returns True if the given country code is in the EU, else False.
    Accepts ISO 3166-1 alpha-2 codes (e.g., 'DE', 'FR').
    """
    if not country_code:
        return False
    return country_code.upper() in EU_COUNTRY_CODES

from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme

@csrf_exempt
def login_view(request):
    print("[DEBUG] Entered login_view")
    if request.session.get('is_authenticated'):
        print("[DEBUG] User already authenticated, redirecting to dashboard:tl_dashboard")
        return redirect('dashboard:tl_dashboard') if 'dashboard' in settings.INSTALLED_APPS else redirect('/')

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
            return HttpResponseRedirect(reverse('dashboard:tl_dashboard'))

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
            #user_entry location entries
            print("\n--- User Location Details ---")
            print("City:", user_entry["l"].value if "l" in user_entry else None)
            print("Office:", user_entry["physicalDeliveryOfficeName"].value if "physicalDeliveryOfficeName" in user_entry else None)
            print("Street:", user_entry["streetAddress"].value if "streetAddress" in user_entry else None)
            print("Postal Code:", user_entry["postalCode"].value if "postalCode" in user_entry else None)
            print("Country:", user_entry["co"].value if "co" in user_entry else None)
            print("Country Code:", user_entry["c"].value if "c" in user_entry else None)
            print("Usage Location:", user_entry["msExchUsageLocation"].value if "msExchUsageLocation" in user_entry else None)
            print("Site Code:", user_entry["extensionAttribute5"].value if "extensionAttribute5" in user_entry else None)
            print("-----------------------------\n")

            # Store user location details in session variables
            request.session['city'] = user_entry["l"].value if "l" in user_entry else None
            request.session['office'] = user_entry["physicalDeliveryOfficeName"].value if "physicalDeliveryOfficeName" in user_entry else None
            request.session['street'] = user_entry["streetAddress"].value if "streetAddress" in user_entry else None
            request.session['postal_code'] = user_entry["postalCode"].value if "postalCode" in user_entry else None
            request.session['country'] = user_entry["co"].value if "co" in user_entry else None
            request.session['country_code'] = user_entry["c"].value if "c" in user_entry else None
            request.session['usage_location'] = user_entry["msExchUsageLocation"].value if "msExchUsageLocation" in user_entry else None
            request.session['site_code'] = user_entry["extensionAttribute5"].value if "extensionAttribute5" in user_entry else None

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
                role = map_role_at_login(request)
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

            home_url = reverse('dashboard:tl_dashboard')
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

# accounts/ldap_utils.py

from ldap3 import Server, Connection, ALL
from django.conf import settings


from ldap3 import Server, Connection, ALL, SUBTREE
from django.conf import settings

def map_role_at_login(request):
    """
    Connects to LDAP using session credentials, fetches reportee count,
    and returns 'TEAM_LEAD' if reportees exist, else 'EMPLOYEE'.
    """
    print("[map_role_at_login] Called")
    ldap_server = getattr(settings, "LDAP_SERVER", None)
    ldap_base_dn = getattr(settings, "LDAP_BASE_DN", None)
    username = request.session.get('ldap_username')  # may be UPN (email-like) or sAMAccountName
    password = request.session.get('ldap_password')

    print(f"[map_role_at_login] ldap_server: {ldap_server}")
    print(f"[map_role_at_login] ldap_base_dn: {ldap_base_dn}")
    print(f"[map_role_at_login] username: {username}")
    print(f"[map_role_at_login] password: {'***' if password else None}")

    if not ldap_server or not ldap_base_dn:
        print("[map_role_at_login] LDAP settings missing, returning EMPLOYEE")
        return "EMPLOYEE"

    if not username or not password:
        print("[map_role_at_login] Username or password missing, returning EMPLOYEE")
        return "EMPLOYEE"

    # Prepare server and connection
    server = Server(ldap_server, get_info=ALL)
    conn = None

    try:
        print("[map_role_at_login] Attempting LDAP connection...")
        # Bind: For AD, user can be UPN (email-like) or DOMAIN\\sAMAccountName depending on config
        conn = Connection(server, user=username, password=password, auto_bind=True)
        print("[map_role_at_login] LDAP connection established")

        # Derive sAMAccountName from username if it's an email/UPN
        # e.g., 'john.doe@example.com' -> 'john.doe'
        if '@' in username:
            sam = username.split('@')[0]
        else:
            sam = username
        print(f"[map_role_at_login] Derived sAMAccountName for search: {sam}")

        # 1) Resolve the actual DN for the logged-in user
        # Prefer sAMAccountName search, but also try userPrincipalName if needed.
        # Note: In some ADs, sAMAccountName may differ; UPN is more stable.
        # Try sAMAccountName first:
        found_user_dn = None
        print("[map_role_at_login] Searching for user DN by sAMAccountName...")
        conn.search(
            search_base=ldap_base_dn,
            search_filter=f"(sAMAccountName={sam})",
            search_scope=SUBTREE,
            attributes=['distinguishedName', 'userPrincipalName', 'cn']
        )
        if conn.entries:
            found_user_dn = conn.entries[0].distinguishedName.value
            print(f"[map_role_at_login] Found user DN via sAMAccountName: {found_user_dn}")
        else:
            print("[map_role_at_login] sAMAccountName search returned no entries, trying UPN...")
            # Try UPN
            conn.search(
                search_base=ldap_base_dn,
                search_filter=f"(userPrincipalName={username})",
                search_scope=SUBTREE,
                attributes=['distinguishedName', 'userPrincipalName', 'cn']
            )
            if conn.entries:
                found_user_dn = conn.entries[0].distinguishedName.value
                print(f"[map_role_at_login] Found user DN via userPrincipalName: {found_user_dn}")
            else:
                print("[map_role_at_login] Could not resolve user DN via sAMAccountName or UPN")
                return "EMPLOYEE"

        user_dn = found_user_dn
        print(f"[map_role_at_login] Using user_dn: {user_dn}")

        # 2) Try reading the user's directReports attribute (if populated)
        print("[map_role_at_login] Reading 'directReports' from user entry...")
        conn.search(
            search_base=user_dn,  # direct DN
            search_filter="(objectClass=user)",
            search_scope=SUBTREE,
            attributes=['directReports']
        )
        direct_reports_from_user = []
        if conn.entries:
            entry = conn.entries[0]
            if 'directReports' in entry and entry.directReports:
                # directReports is a list of DNs of direct reportees
                direct_reports_from_user = list(entry.directReports)
                print(f"[map_role_at_login] directReports attribute present, count: {len(direct_reports_from_user)}")
            else:
                print("[map_role_at_login] directReports attribute not present or empty")
        else:
            print("[map_role_at_login] Could not read user entry at DN")

        # 3) Also search for objects where manager=<user_dn>
        # This is the most reliable way to count direct reports in AD
        search_filter = f"(&(objectClass=user)(manager={user_dn}))"
        print(f"[map_role_at_login] Searching reportees with filter: {search_filter}")
        # Attributes list can be tuned if needed, keep minimal for performance
        attributes = getattr(settings, "LDAP_ATTRIBUTES", [
            'cn', 'sAMAccountName', 'userPrincipalName', 'mail', 'department',
            'title', 'manager'
        ])

        conn.search(
            search_base=ldap_base_dn,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=attributes
        )

        reportees_by_manager_filter = conn.entries or []
        print(f"[map_role_at_login] Found {len(reportees_by_manager_filter)} entries via manager DN filter")

        # 4) Consolidate counts (some environments only populate one of these)
        reportee_count = max(len(direct_reports_from_user), len(reportees_by_manager_filter))
        print(f"[map_role_at_login] Final reportee_count: {reportee_count}")

        role = "TEAM_LEAD" if reportee_count > 0 else "EMPLOYEE"
        print(f"[map_role_at_login] Returning role: {role}")
        return role

    except Exception as e:
        print(f"[map_role_at_login] Exception occurred: {e}")
        return "EMPLOYEE"

    finally:
        if conn:
            try:
                print("[map_role_at_login] Unbinding LDAP connection")
                conn.unbind()
            except Exception as unbind_err:
                print(f"[map_role_at_login] Unbind error: {unbind_err}")
