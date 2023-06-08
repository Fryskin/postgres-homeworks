"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


connection = psycopg2.connect(host='localhost', database='north', user='postgres', password='bubacock')

try:
    with connection.cursor() as cursor:

        with open('/postgres-homeworks/homework-1/north_data/customers_data.csv') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if len(row[0]) != 5:
                    continue
                else:
                    cursor.execute("INSERT INTO customers VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
                    connection.commit()
        with open('/postgres-homeworks/homework-1/north_data/employees_data.csv') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if not row[0].isdigit():
                    continue

                else:
                    cursor.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2],
                                                                                             row[3], row[4], row[5]))
                    connection.commit()
        with open('/postgres-homeworks/homework-1/north_data/orders_data.csv') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:

                if not row[0].isdigit():
                    continue

                else:
                    cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", (row[0], row[1], row[2], row[3],
                                                                                      row[4]))
                    connection.commit()
finally:
    connection.close()
