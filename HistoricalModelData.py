import SP500Data as sp
import yfinance as yf
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
import psycopg2
import LoginInformation

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
        cur.execute('''SELECT * FROM "Momentum".stockdata
                        ORDER BY date desc''')

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

def create_data(data_df):
    unique_symbols = data_df["symbol"].unique()
    data_df['volumeTrend'] = np.nan
    data_df['priceTrend'] = np.nan
     # Iterate through unique symbols
    for symbol in unique_symbols:
        symbol_data = data_df[data_df["symbol"] == symbol]
        print(symbol)
        for i in range(len(symbol_data)):
            data = symbol_data.iloc[i:180+i]
            data.reset_index(drop=True, inplace=True)
            last_date = data.iloc[0]["date"]
            volumetrend = get_volume_trend(data)
            pricetrend = get_price_trend(data)
            if 180 + i > len(symbol_data):
                break
            matching_row = data_df[(data_df["date"] == last_date) & (data_df["symbol"] == symbol)]
            data_df.loc[matching_row.index, "volumeTrend"] = volumetrend
            data_df.loc[matching_row.index, "priceTrend"] = pricetrend
    return data_df
        

def standardize_data(data_df):
    data_df['VolumeStandard'] = np.nan
    data_df['PEStandard'] = np.nan
    data_df['PriceStandard'] = np.nan
    data_df['PE Volume Price Combined'] = np.nan


    unique_dates = data_df['date'].unique()
    for date in unique_dates:
        data = data_df[data_df["date"] == date]
        data = standardize(data)
        data_df.loc[data.index, 'VolumeStandard'] = data['VolumeStandard']
        data_df.loc[data.index, 'PriceStandard'] = data['PriceStandard']
        data_df.loc[data.index, 'PEStandard'] = data['PEStandard']
        data_df.loc[data.index, 'PE Volume Price Combined'] = data['PE Volume Price Combined']
    return data_df




def get_volume_trend(data_df):
    data = data_df.iloc[-30:, :]  # Select last 30 rows using slicing
    data = data.reset_index(drop=True)    # Ensure the data is sorted by date
    
    data = data.sort_values('date')
    data['RowNum'] = data.index

    # Reshape data for sklearn
    X = data['RowNum'].values.reshape(-1, 1)
    y = data['volume'].values
    
    # Fit linear regression model
    model = LinearRegression()
    model.fit(X, y)
    
    # Return the slope of the trend
    return model.coef_[0]



def get_price_trend(data_df):
    data = data_df.iloc[-180:, :]  # Select last 180 rows using slicing
    data = data.reset_index(drop=True)    # Ensure the data is sorted by date

    # Ensure the data is sorted by date
    data = data.sort_values('date')
    data['RowNum'] = data.index

    # Reshape data for sklearn
    X = data['RowNum'].values.reshape(-1, 1)
    y = data['price'].values
    
    # Fit linear regression model
    model = LinearRegression()
    model.fit(X, y)
    
    # Return the slope of the trend
    return model.coef_[0]

    
def standardize(data_df):
    columnstoStandardize = ['pe_ratio', 'volumeTrend', 'priceTrend']
    standardizedColumns = ['PEStandard', 'VolumeStandard', 'PriceStandard']
    for original, new in zip(columnstoStandardize, standardizedColumns):
        data_df[original] = data_df[original].astype(float)
        mean = float(data_df[original].mean())
        std = float(data_df[original].std())
        bottom = float(mean - 2 * std)
        top = float(mean + 2 * std)

        data_df.loc[data_df[original] > top, new] = 1
        data_df.loc[data_df[original] < bottom, new] = -1

        filtered_data = data_df[(data_df[original] >= bottom) & (data_df[original] <= top)]
        max = float(filtered_data[original].max())
        min = float(filtered_data[original].min())
        data_df.loc[(data_df[original] >= bottom) & (data_df[original] <= top), new] = ((data_df[original] - min) / (max - min)) * 2 - 1

    data_df['PEStandard'] = data_df['PEStandard'] * -1
    data_df['PEStandard'] = data_df['PEStandard'].astype(float)
    data_df['VolumeStandard'] = data_df['VolumeStandard'].astype(float)
    data_df['PriceStandard'] = data_df['PriceStandard'].astype(float)


    data_df['PE Volume Price Combined'] = ((data_df['PEStandard']) + (data_df['VolumeStandard']) + (data_df['PriceStandard']))
    return data_df


def add_to_sql(data_df):
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

            for row in data_df.values:
                date = f"'{row[1]}'"
                symbol = f"'{row[0]}'"
                price = row[2]
                volume = row[3]
                
                insertString = f'''Insert into "Momentum".modeldata (symbol, date, price, volume, pe_ratio, volume_trend, price_trend, volume_standard, pe_standard, price_standard, pe_price_volume_combined) values({symbol},{date},{price},{volume},'''
        
                i = 4
                while i < 11:
                    if type(row[i]) == str or row[i] == None or np.isnan(float(row[i])):
                        insertString += 'NULL'
                        if i != 10:
                            insertString += ','
                    else:
                        insertString += str(row[i])
                        if i != 10:
                            insertString += ','
                    i+=1
                insertString += ")"
                cur.execute(insertString)
            
            print(f"SQL Executed: {len(data_df.values)} rows added")

            # Close the cursor and connection
            conn.commit()
            cur.close()
            conn.close()
        except Exception as error:
            print(f"Error: {error}")



if __name__ == '__main__':
    df = get_data()
    df = create_data(df)
    df = standardize_data(df)
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')  # Adjust format (YYYY-MM-DD)
    #df = df[df['date'] > '2024-07-05']
    add_to_sql(df)
   
   
   # print(df)
   # pd.set_option('display.max_columns', None)
   # pd.set_option('display.max_rows', 100)
   # df['date'] = pd.to_datetime(df['date'])
   # df_filtered = df[df['date'] == '2024-07-05']
  #  print(df_filtered)




    
