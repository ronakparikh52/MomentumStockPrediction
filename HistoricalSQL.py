import psycopg2
import GetHistoricalData
import LoginInformation
import numpy as np
import LoginInformation


data = GetHistoricalData.finalizeData()


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
    for row in data.values:
        date = f"'{row[0]}'"
        symbol = f"'{row[1]}'"
        price = row[3]
        volume = row[4]

        if type(row[2]) == str or np.isnan(float(row[2])):
            insertString = f'Insert into "Momentum"."stockdata" values ({symbol},{date},{price},{volume}, NULL);'
        else:
            pe_ratio = row[2]
            insertString = f'Insert into "Momentum"."stockdata" values ({symbol},{date},{price},{volume},{pe_ratio});'

        cur.execute(insertString)

    totalCompanies = len(data.values)
    print(f"Execution Completed: {totalCompanies}")

    # Close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()

except Exception as error:
    print(f"Error: {error}")
