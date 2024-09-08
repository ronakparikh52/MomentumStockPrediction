import requests
from bs4 import BeautifulSoup
import pandas as pd


def SP500():
    #Use Wikipedia parsing to get latest S&P 500 companies

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"class": "wikitable sortable"})  # Adjust class name if needed

    column_names = ['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date Added', 'CIK', 'Founded']
    sp500_companies = []


    # Loop through table rows (skipping the header row)
    for row in table.find_all("tr")[1:]:
        company_data = []
        for cell in row.find_all("td"):
            # Extract text from table cells
            text = cell.text.strip()
            company_data.append(text)
        sp500_companies.append(company_data)


    sp500_df = pd.DataFrame(sp500_companies, columns = column_names)
    return sp500_df

def getSector(sp500_df):
    specific_sector_df = sp500_df[sp500_df['GICS Sector'] == 'Information Technology']
    selected_columns = ['Symbol', 'Security']
    specific_companies_df = specific_sector_df[selected_columns]
    return specific_companies_df