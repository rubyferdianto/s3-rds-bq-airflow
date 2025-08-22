import pymysql

# --- Configuration ---
RDS_HOST = "hostname"
RDS_PORT = 3306
USERNAME = "admin"  # RDS master user
PASSWORD = "password"
DATABASE_TO_DROP = "database_name"  # database you want to drop

# --- Connect to RDS (without specifying a database) ---
try:
    connection = pymysql.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        user=USERNAME,
        password=PASSWORD,
        cursorclass=pymysql.cursors.DictCursor
    )

    with connection.cursor() as cursor:
        # Drop the database
        sql = f"DROP DATABASE IF EXISTS `{DATABASE_TO_DROP}`;"
        cursor.execute(sql)
        print(f"Database '{DATABASE_TO_DROP}' has been dropped successfully.")

    connection.commit()

except pymysql.MySQLError as e:
    print(f"Error: {e}")

finally:
    if connection:
        connection.close()
