# import cx_Oracle
import os
import pandas as pd
import cx_Oracle as orcCon
# import getpass

user = os.environ.get("PYTHON_USER", "system")

dsn = os.environ.get("PYTHON_CONNECT_STRING", "localhost/XE")

pw = os.environ.get("PYTHON_PASSWORD")
if pw is None:
    pw = 'ada8970884'

data = pd.read_csv('data/products_short.csv', sep = '\t')
data.columns = [i.replace('-', '_') for i in data.columns]
data.fillna(0, inplace = True)

# import cx_Oracle as orcCon
from cx_Oracle import DatabaseError
try:
    conn = orcCon.connect(user, pw, dsn)
    if conn:
        print("cx_Oracle version:", orcCon.version)
        print("Database version:", conn.version)
        print("Client version:", orcCon.clientversion())

        # Now execute the sqlquery
        cursor = conn.cursor()
        print("You're connected.................")

        # Drop table if exists
        print('Droping table if exists............')
        cursor.execute("BEGIN EXECUTE IMMEDIATE 'DROP TABLE product'; EXCEPTION WHEN OTHERS THEN NULL; END;")

        print('Creating table product............')
        # cursor.execute("CREATE TABLE iris (sepal_length number(3,1) NOT NULL, sepal_width number(3,1) NOT NULL, petal_length number(3,1) NOT NULL, petal_width number(3,1),species varchar2(10) NOT NULL)")
        cursor.execute("CREATE TABLE product (code varchar2(40) NOT NULL, energy_kcal_100g number, energy_100g number, fat_100g number, saturated_fat_100g number, carbohydrates_100g number, sugars_100g number, proteins_100g number, salt_100g number, sodium_100g number)")
        print("product table is created..............")
except DatabaseError as e:
    err = e.args
    print("Oracle-Error-Code:", err.code)
    print("Oracle-Error-Message:", err.message)

finally:
    cursor.close()
    conn.close()

cols = ['code',
 'energy_kcal_100g',
 'energy_100g',
 'fat_100g',
 'saturated_fat_100g',
 'carbohydrates_100g',
 'sugars_100g',
 'proteins_100g',
 'salt_100g',
 'sodium_100g']

try:
    #orcCon.connect('username/password@localhost')
    conn = orcCon.connect(user, pw, dsn)
    if conn:
        print("cx_Oracle version:", orcCon.version)
        print("Database version:", conn.version)
        print("Client version:", orcCon.clientversion())
        cursor = conn.cursor()
        print("You're connected: ")
        print('Inserting data into table....')
        print()
        for i,row in data[cols].iterrows():
            # sql = "INSERT INTO iris(sepal_length,sepal_width,petal_length,petal_width,species) VALUES(:1,:2,:3,:4,:5)"
            sql = "INSERT INTO product(code,energy_kcal_100g,energy_100g,fat_100g,saturated_fat_100g, carbohydrates_100g,sugars_100g,proteins_100g,salt_100g,sodium_100g) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10) "
            cursor.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()
        print("Record inserted succesfully")
except DatabaseError as e:
    err = e.args
    print("Oracle-Error-Code:", err.code)
    print("Oracle-Error-Message:", err.message)
#
finally:
    cursor.close()
    conn.close()



# cur = con.cursor()
# cur.execute("select * from dept order by deptno")
# for deptno, dname, loc in cur:
#     print("Department number: ", deptno)
#     print("Department name: ", dname)
#     print("Department location:", loc)
#
# print("Database version:", con.version)