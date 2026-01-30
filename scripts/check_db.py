from atlas.db import get_conn, init_schema, init_indexes, check_db_health



def test_db_connection():
    conn = get_conn()
    conn.close()


def test_schema_and_health():
    init_schema()
    init_indexes()
    check_db_health()


if __name__ == "__main__":
    print("Running DB tests...")

    test_db_connection()
    test_schema_and_health()

    print("DB layer ready ✅")
