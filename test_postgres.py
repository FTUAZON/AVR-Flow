import psycopg


connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="AVR-Flow",
    user="postgres",
    password="root",
)


with connection.cursor() as cursor:
    cursor.execute(
        "SELECT current_database();"
    )

    result = cursor.fetchone()

    print(
        "Connected to database:",
        result[0],
    )


connection.close()