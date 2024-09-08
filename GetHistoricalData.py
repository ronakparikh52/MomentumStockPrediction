import SP500Data as sp
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine

#Gets the data for volume and price everyday for the past 10 years of a stock
def getData(symbol):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="10y")  # Download last year's data
    data_df = pd.DataFrame(data)

    #Manipulating Date in dataframe
    data_df['Date'] = data_df.index
    data_df = data_df.reset_index(drop=True)  # Reset index, dropping the old one
    data_df['Date'] = data_df['Date'].dt.strftime('%Y-%m-%d')  # Adjust format as needed (YYYY-MM-DD is common)
    selected_columns = ['Date', 'Close', 'Volume']
    data_df = data_df[selected_columns]
    data_df['Symbol'] = symbol  # Replace 'AAPL' with your desired symbol
    return data_df

#Gets the Historical PE Ratios, reading a csv that got the data form faxset, for the past 10 years of all current S&P 500 tech stocks
def getHistoricalPERatios():
    df_PE_Ratios = pd.read_csv('PEData.csv')
    company_list = df_PE_Ratios.columns[1:]
    datesArray = []
    symbol = []
    PE_Ratios = []
    for company in company_list:
        datesArray.append(df_PE_Ratios['Date'])

        for ratio in df_PE_Ratios[company]:
           symbol.append(company)
           PE_Ratios.append(ratio)           
        
    datesArray = np.concatenate(datesArray)


    formatted_dates = []
    # Define the date string
    for date in datesArray:
        date_obj = datetime.datetime.strptime(date, "%m/%d/%y")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        formatted_dates.append(formatted_date)
    
    datesArray = formatted_dates

    newPE_Ratios = pd.DataFrame({
    'Date': datesArray,
    'Symbol': symbol,
    'PE Ratio': PE_Ratios  # Assuming PE_Ratios refers to your PE ratios list
    })
    return newPE_Ratios

#finalize and return the final dataset
def finalizeData():
    SP500 = sp.SP500()
    sectorData = sp.getSector(SP500)
    combined_df = pd.DataFrame()
    PE_Ratios = getHistoricalPERatios()

    for company in sectorData.values:
        symbol = company[0]
        company_df = getData(symbol)
        combined_df = pd.concat([combined_df, company_df], ignore_index=True)

    merged_df = pd.merge(PE_Ratios, combined_df, on=['Date', 'Symbol'], how='inner')
    merged_df['Date'] = pd.to_datetime(merged_df['Date'], format='%Y-%m-%d')
    return merged_df




if __name__ == '__main__':
    finalizedData = finalizeData()
    print(finalizedData)




    

