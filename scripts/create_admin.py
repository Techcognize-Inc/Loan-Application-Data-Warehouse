from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

with get_application_builder() as appbuilder:
    auth_manager = FabAuthManager()
    auth_manager.appbuilder = appbuilder
    sm = auth_manager.security_manager

    role = sm.find_role("Admin")
    if not role:
        role = sm.add_role("Admin")

    existing = sm.find_user(username="admin")
    if existing:
        print("Admin user already exists — skipping")
    else:
        sm.add_user(
            username="admin",
            first_name="Admin",
            last_name="User",
            email="admin@example.com",
            role=role,
            password="admin",
        )
        print("Admin user created successfully")
