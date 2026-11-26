
from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)


with get_db_connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                current_database(),
                current_user,
                version()
        """)

        database, user, version = cursor.fetchone()

        print("Database:", database)
        print("User:", user)
        print("PostgreSQL:", version.split(",")[0])

        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'ingestion'
            ORDER BY table_name
        """)

        tables = [row[0] for row in cursor.fetchall()]

        assert tables == [
            "channels",
            "ingested_videos",
            "ingestion_runs",
            "ingestion_tasks",
        ]

        print("Tables:", tables)
        print("PostgreSQL connection test: PASSED")
