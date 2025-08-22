from mysql.connector import connect, Error


#icsdid = '180'
try:
    with connect(
        host="mysql-icsd.science.ru.nl",
        user="icsd_reader",
        password="ePnV3Od6u0rbQUVa",
        database="icsd",
    ) as connection:
        select_query = """
        SELECT 
        icsd.idnum,
        icsd.sum_form 
        FROM icsd 
        WHERE 
        ((SUM_FORM REGEXP '(^|\ )Mn[0-9\.]+($|\ )') OR 
        (SUM_FORM REGEXP '(^|\ )Fe[0-9\.]+($|\ )') OR
        (SUM_FORM REGEXP '(^|\ )Co[0-9\.]+($|\ )') OR
        (SUM_FORM REGEXP '(^|\ )Ni[0-9\.]+($|\ )') OR
        (SUM_FORM REGEXP '(^|\ )Cr[0-9\.]+($|\ )')) AND (
        NOT(SUM_FORM REGEXP '(^|\ )O[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Re[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Os[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Ir[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Pt[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Au[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )In[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Tc[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Be[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )As[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Cd[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Ba[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Hg[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Tl[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Pb[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Ac[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Cs[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Po[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Np[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )U[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Pu[0-9\.]+($|\ )') AND
        NOT(SUM_FORM REGEXP '(^|\ )Th[0-9\.]+($|\ )') )
        ;"""

        with connection.cursor() as cursor:
            cursor.execute(select_query)
            records = cursor.fetchall()
            print('Collecting ids for ' + str(len(records)) + ' compositions')
            with open('datalist.csv', "w") as id_file:
                id_file.write('ID,compound,pretty_formula,mag_field\n')
                for row in records:
                    id_file.write(str(row[0]).strip('\n') + ',' + str(row[1]).strip('\n') + ',' + str(row[1]).strip('\n') + ',' + '0' + '\n')
        connection.close()

except Error as e:
    print(e)
