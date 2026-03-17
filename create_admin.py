# Airflow 3 admin user creation
# CLI commands (airflow users, airflow fab) were removed in Airflow 3.
# User creation is done via the FAB security manager Python API.

from airflow import settings
from sqlalchemy.orm import Session

with Session(settings.engine) as session:
    try:
        from airflow.providers.fab.auth_manager.models import User
        existing = session.query(User).filter_by(username="admin").first()
        if existing:
            print("Admin user already exists — skipping creation")
        else:
            from airflow.www.app import create_app
            app = create_app()
            with app.app_context():
                sm = app.appbuilder.sm
                role = sm.find_role("Admin")
                sm.add_user(
                    username="admin",
                    first_name="Admin",
                    last_name="User",
                    email="admin@example.com",
                    role=role,
                    password="admin",
                )
                print("Admin user created successfully")
    except Exception as e:
        print(f"Warning: could not create admin user: {e}")
        print("You can create it manually: docker exec -it airflow-webserver airflow users create ...")
