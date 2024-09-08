import SP500Data as sp
import yfinance as yf
import pandas as pd

#Gets the data for volume and price everyday for the past 10 years of a stock
def getData(symbol):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1mo")  # Download last year's data
    data_df = pd.DataFrame(data)
    try:
        priceEarnings = ticker.info['trailingPE']
    except KeyError:
        # Handle missing 'trailingPE' key
        priceEarnings = None 
    #Reorganizing Date in dataframe
    data_df['Date'] = data_df.index
    data_df = data_df.reset_index(drop=True)  # Reset index, dropping the old one
    data_df['Date'] = data_df['Date'].dt.strftime('%Y-%m-%d')  # Adjust format (YYYY-MM-DD)
    data_df['PriceToEarnings'] = priceEarnings
    data_df['Symbol'] = symbol 


    selected_columns = ['Symbol', 'Date', 'Close', 'Volume', 'PriceToEarnings']
    data_df = data_df[selected_columns]
    return data_df

#Finalize and return the final dataset
def finalizeData():
    SP500 = sp.SP500()
    sectorData = sp.getSector(SP500)
    final_df = pd.DataFrame()
    for company in sectorData.values:
        symbol = company[0]
        company_df = getData(symbol)
        final_df = pd.concat([final_df, company_df], ignore_index=True)

    #print(final_df.info())
    #nan_rows = final_df.loc[final_df.isna().any(axis=1)]
    #print(nan_rows)
    final_df = final_df[final_df['Date'] > '2024-07-05']

    return final_df



if __name__ == '__main__':
    print(finalizeData())



    

