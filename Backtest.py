
import pandas as pd
import psycopg2
import LoginInformation
from scipy import stats

def get_data():

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

        # Execute a query to fetch all data from a table (replace "your_table" with your actual table name)
        cur.execute(
            '''SELECT pcd.symbol, pcd.date, pcd.price, pcd.pe_ratio, avgpe.avg_pe_ratio,
                pcd.five_day, pcd.ten_day, pcd.fifteen_day, pcd.thirty_day, pcd.sixty_day, pcd.ninety_day, pcd.onehundredeighty_day,
                md.volume_standard, md.price_standard, md.pe_standard, md.pe_price_volume_combined
                FROM "Momentum".price_comparison_data AS pcd
                INNER JOIN "Momentum".modeldata AS md
                    ON pcd.symbol = md.symbol AND pcd.date = md.date
                INNER JOIN "Momentum".avg_pe_bydate AS avgpe
                    ON pcd.date = avgpe.date
                WHERE pcd.pe_ratio < avgpe.avg_pe_ratio AND md.pe_price_volume_combined IS NOT NULL;
        ''')

        # Fetch all rows
        rows = cur.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cur.description]

        # Create a pandas DataFrame from the fetched data
        df = pd.DataFrame(rows, columns=column_names)


        # Close the cursor and connection
        conn.commit()
        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error: {error}")
    return df





def model(data_df):
    price_columns = ["five_day", "ten_day", "fifteen_day", "thirty_day", "sixty_day", "ninety_day", "onehundredeighty_day"]
    variable_columns = ["price_standard", "pe_standard", "volume_standard", "pe_price_volume_combined"]
    for price_column in price_columns:
        price_difference = data_df[price_column] - data["price"]
        print()
        print()
        print(price_column)
        print()
        for variable_column in variable_columns:
            variable_data = data[variable_column]
            correlation, p_value = stats.pearsonr(variable_data.astype(float), price_difference.astype(float))
            print(f"{variable_column}: Correlation: {correlation}, p_value: {p_value}")


data = get_data()
print(data)
model(data)