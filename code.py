import psycopg2
import pandas

# connect to bd
conn = psycopg2.connect(host='localhost', dbname='north', user='oleg', password='12345')

# reading csv
emp_data = pandas.read_csv('employees_data.csv')
cust_data = pandas.read_csv('customers_data.csv')
ord_data = pandas.read_csv('orders_data.csv')


try:
    with conn:
        with conn.cursor() as cur:
            cur.execute('''
                        DELETE FROM orders;
                        DELETE FROM employeess;
                        ALTER SEQUENCE employeess_employees_id_seq RESTART WITH 1;
                        DELETE FROM customers
                        ''')
            '''Cleared db.'''

            for i in range(len(emp_data)):
                cur.execute(f'INSERT INTO employeess (first_name,last_name,title,birth_day,notes) VALUES {tuple((emp_data.loc[i]))}')
            for i in range(len(cust_data)):
                cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', list((cust_data.loc[i])))
            for i in range(len(ord_data)):
                a = list((ord_data.loc[i]))
                a[0], a[2] = int(a[0]), int(a[2]) ## костыль, но без этого не работает
                cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', a)
            '''записываем данные в db'''


            cur.execute('SELECT * FROM customers')
            rows = cur.fetchall()
            for row in rows:
                print(row)

            cur.execute('SELECT * FROM employeess')
            rows = cur.fetchall()
            for row in rows:
                print(row)

            cur.execute('SELECT * FROM orders')
            rows = cur.fetchall()
            for row in rows:
                print(row)


finally:
    conn.close()


