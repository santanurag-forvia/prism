# FEAS (Forvia Enterprise Allocation System) - AI Coding Instructions

## Project Overview
FEAS is a Django-based enterprise resource allocation system with LDAP/Active Directory integration, designed for project management and employee utilization tracking at Forvia.

## Architecture & Key Components

### Core Apps Structure
- **accounts/**: LDAP authentication, user session management, reportee hierarchy
- **dashboard/**: Role-based dashboards with utilization metrics and charts
- **projects/**: Project management and allocation tracking  
- **allocations/**: Resource allocation entries and calculations
- **resources/**: Resource management
- **settings/**: Application configuration
- **base/**: Shared utilities and base templates

### Authentication & Session Management
- **LDAP Integration**: Uses `ldap3` for Active Directory authentication via `accounts/ldap_utils.py`
- **Session-based Auth**: No Django User model - all user data stored in sessions
- **Role Mapping**: Maps LDAP attributes to internal roles (ADMIN, PDL, TEAM_LEAD, COE_LEADER, EMPLOYEE)
- **Key Session Variables**: `is_authenticated`, `ldap_username`, `cn`, `title`, `role`, `ldap_password`

### Database Patterns
- **MySQL Backend**: Uses mysql-connector-python with raw SQL queries via `dict_fetchall()`
- **Schema Initialization**: `feas_project/db_initializer.py` handles database setup and seeding
- **No Django ORM**: Most database operations use raw SQL through `django.db.connection`

### Role-Based Access Control
- **Menu System**: Dynamic navigation via `accounts/context_processors.py` with role-based visibility
- **View Protection**: Role checks in views using `request.session.get("role")`
- **Template Context**: User info injected globally via context processor

## Development Patterns

### Django Settings (`feas_project/settings.py`)
```python
# LDAP Configuration
LDAP_SERVER = '10.170.130.91'
LDAP_BASE_DN = 'DC=ls,DC=ege,DC=ds' 
LDAP_ATTRIBUTES = ['cn', 'sAMAccountName', 'userPrincipalName', 'mail', ...]

# Superadmin Override
FEAS_SUPERADMIN_USERNAME = os.getenv('FEAS_SUPERADMIN_USERNAME', 'admin')
```

### View Patterns
```python
# Standard view structure
def view_name(request):
    user_role = request.session.get("role", "EMPLOYEE")
    user_ldap = request.session['ldap_username']
    
    # Raw SQL pattern
    sql = "SELECT ... FROM table WHERE user_ldap = %s"
    data = dict_fetchall(sql, (user_ldap,))
    
    return render(request, "app/template.html", context)
```

### LDAP Operations
- **User Lookup**: `get_user_entry_by_username()` in `accounts/ldap_utils.py`
- **Reportee Hierarchy**: `get_reportees_for_user_dn()` for manager relationships
- **Connection Management**: Uses service account fallback when user credentials unavailable

### Template Structure
- **Base Template**: `templates/base.html` with Forvia branding and user dropdown
- **Static Assets**: App-specific CSS/JS in `static/{app_name}/`
- **Font Awesome**: CDN-loaded for icons throughout UI

## Key Integration Points

### URL Routing
- Root redirects to accounts (login)
- Namespaced URLs: `dashboard:home`, `projects:list`, etc.
- Admin interface at `/admin/`

### Database Schema
- **monthly_allocation_entries**: Core allocation tracking table
- **system_settings**: Initialization flag storage
- **Raw SQL Focus**: Direct table access rather than Django models

### External Dependencies
- **Chart.js**: Client-side data visualization
- **openpyxl/xlsxwriter**: Excel export functionality
- **pandas**: Data processing for reports

## Development Workflow

### Environment Setup
```bash
# Virtual environment with Python 3.13
pip install -r requirements.txt
python manage.py runserver
```

### Database Initialization
- Auto-runs via `initialize_database()` in views
- Idempotent - safe to call multiple times
- Creates tables and seeds lookup data

### Testing LDAP
- Superadmin bypass: username/password both "admin" (configurable via env)
- Real LDAP testing requires domain connectivity

## Common Patterns to Follow

1. **Session Management**: Always check `request.session.get('is_authenticated')` for protected views
2. **Role Checks**: Use session role for authorization, not LDAP queries in views  
3. **LDAP Connections**: Always unbind connections in try/finally blocks
4. **Raw SQL**: Use `dict_fetchall()` helper for consistent result formatting
5. **Template Context**: Leverage global context processor for user info rather than manual injection

## Critical Files for Understanding
- `feas_project/settings.py`: Core configuration and LDAP settings
- `accounts/views.py`: Authentication flow and session management
- `accounts/ldap_utils.py`: LDAP integration patterns
- `dashboard/views.py`: Role-based data access patterns
- `accounts/context_processors.py`: Menu system and global context
- `templates/base.html`: UI structure and user session display