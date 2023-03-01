import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(40),
            last_name VARCHAR(40),
            email VARCHAR(50) UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone(
            phone_id SERIAL PRIMARY KEY,
            number VARCHAR(14) UNIQUE,
            client_id INT REFERENCES client(client_id)
        );
        """)
        conn.commit()
    return True

def add_client(conn, first_name, last_name, email):
    with conn.cursor() as cur:
        try:
            cur.execute("""
            INSERT INTO client(first_name, last_name, email) 
            VALUES
                (%s, %s, %s);
            """, (first_name, last_name, email))
            conn.commit()
            print('Client added successfully')
            return True
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            print(f"Error: The email '{email}' already exists in the database")
            return False

def add_phone(conn, client_id, number):
    with conn.cursor() as cur:
        try:
            cur.execute("""
            INSERT INTO phone(number, client_id)
            VALUES
                (%s, %s);
            """, (number, client_id))
            conn.commit()
            print('Phone number added successfully')
            return True
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            print(f"Error: The phone number '{number}' already exists in the database")
            return False


def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        params = []
        if first_name is not None:
            params.append("first_name = %s")
        if last_name is not None:
            params.append("last_name = %s")
        if email is not None:
            params.append("email = %s")
        
        query = f"""
        UPDATE client
        SET {', '.join(params)}
        WHERE client_id = %s
        """
        values = [value for value in (first_name, last_name, email, client_id) if value is not None]
        if not values:
            print("Error: no values provided for update")
            return False
        
        cur.execute(query, tuple(values))
        if cur.rowcount > 0:
            conn.commit()
            print("Client updated successfully")
            return True
        else:
            conn.rollback()
            print("Error: client not found")
            return False


def delete_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone WHERE client_id = %s AND number = %s;
        """, (client_id, number))
        if cur.rowcount > 0:
            conn.commit()
            print('Phone number deleted successfully')
            return True
        else:
            conn.rollback()
            print('Error: Phone number not found')
            return False

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM client WHERE client_id = %s;
        """, (client_id,))
        if cur.rowcount > 0:
            conn.commit()
            print('Client deleted successfully')
            return True
        else:
            conn.rollback()
            print('Error: Client not found')
            return False

def find_client(conn, first_name=None, last_name=None, email=None, number=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT client.first_name, client.last_name, client.email, STRING_AGG(phone.number, ', ')
        FROM client
        LEFT JOIN phone ON client.client_id = phone.client_id
        WHERE client.first_name = %s OR client.last_name = %s OR client.email = %s OR phone.number = %s
        GROUP BY client.first_name, client.last_name, client.email;
        """, (first_name, last_name, email, number))
        rows = cur.fetchall()
        if rows:
            print("Client information:")
            for row in rows:
                print(f"First name: {row[0]}, Last name: {row[1]}, Email: {row[2]}, Phone number(s): {row[3]}")
            return True
        else:
            print("Client not found")
            return False



if __name__ == '__main__':
    with psycopg2.connect(database="CRM", user="postgres", password="71923") as conn:
        if create_db(conn):
            print('Tables created successfully')
        else:
            print('Error creating tables')

        add_client(conn, 'John', 'Legend', 'john_legend@gmail.com')
        add_phone(conn, 1, '+79982567878')
        delete_phone(conn, 1, '+79982567878')
        delete_client(conn, 4)
        find_client(conn, first_name='John')
        change_client(conn, 1, last_name='Malkovich', email='john_malkovich@gmail.com')

    conn.close()

