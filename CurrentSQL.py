import psycopg2
import GetCurrentData
import numpy as np
import LoginInformation
import CurrentModelData as cmd
data = GetCurrentData.finalizeData()

try:
    # Connect to your postgres DB
    conn = psycopg2.connect(
        host= LoginInformation.host,
        database= LoginInformation.database,
        user= LoginInformation.user,
        password= LoginInformation.password
    )

    # Create a cursor object
    cur = conn.cursor()
    # Execute a query using the schema name
    numberOfNons = 0
    pe_ratio_list = []
    current_date = None
    datecollection = []
    for row in data.values:
        date = f"'{row[1]}'"
        symbol = f"'{row[0]}'"
        price = row[2]
        volume = row[3]


        if type(row[4]) == str or row[4] == None or np.isnan(float(row[4])):
            insertString = f'Insert into "Momentum"."stockdata" values ({symbol},{date},{price},{volume}, NULL);'
            numberOfNons +=1
            print(symbol)
        else:
            pe_ratio = row[4]
            pe_ratio_list.append(pe_ratio)
            insertString = f'Insert into "Momentum"."stockdata" values ({symbol},{date},{price},{volume},{pe_ratio});'
       # cur.execute(insertString)
        datecollection.append(date)
    dateset = set(datecollection)
    avg_pe = sum(pe_ratio_list) / len(pe_ratio_list)
    for date in dateset:
        print(date)
        string2 = f'Insert into "Momentum"."avg_pe_bydate" values ({date}, {avg_pe})'
        cur.execute(string2)
    totalCompanies = len(data.values)
    print(f"Execution Completed: {totalCompanies}")
    print(f"Number of PE Ratio Nulls: {numberOfNons}")
    print(f"Day Avg PE Ratio: {avg_pe}")

    # Close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()

   # cmd.get_current_model_data()


except Exception as error:
    print(f"Error: {error}")
