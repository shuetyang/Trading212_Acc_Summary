import pandas as pd


def calc_return(holding):
    avg_price_gbp = 0.0
    avg_price_market = 0.0
    result_gbp = 0.0
    result_market = 0.0
    fx_impact = 0.0
    dividend = 0.0
    deposit = 0.0
    withdrawal = 0.0
    no_of_share = 0.0
    accum_cost_gbp = 0.0
    accum_cost_market = 0.0

    # New columns to be added
    lst_no_of_share = []
    lst_avg_price_market = []
    lst_avg_price_gbp = []
    lst_result_market = []
    lst_result_gbp = []
    lst_fx_impact = []
    lst_dividend = []
    lst_withdrawal = []
    lst_deposit = []

    for index, row in holding.iterrows():

        # No of shares
        if row['Action'] == 'Buy':
            no_of_share += row['No. of shares']
            # Update average price after buying
            accum_cost_market += row['No. of shares'] * row['Price / share']
            accum_cost_gbp += row['No. of shares'] * row['Price / share'] / row['Exchange rate']
            avg_price_market = accum_cost_market / no_of_share
            avg_price_gbp = accum_cost_gbp / no_of_share

        if row['Action'] == 'Sell':
            result_market += (row['Price / share'] - avg_price_market) * row['No. of shares']
            result_gbp += (row['Price / share'] / row['Exchange rate'] - avg_price_gbp) * row['No. of shares']
            fx_impact += ((row['Price / share'] / row['Exchange rate'] - avg_price_gbp) * row['No. of shares']) - (
                    (row['Price / share'] - avg_price_market) / row['Exchange rate'] * row['No. of shares'])
            no_of_share += row['No. of shares'] * -1
            if no_of_share == 0:
                accum_cost_gbp = 0.0
                accum_cost_market = 0.0
                avg_price_market = 0.0
                avg_price_gbp = 0.0

        if row['Action'] == 'Dividend':
            dividend += row['Total (GBP)']

        if row['Action'] == 'Withdrawal':
            withdrawal += row['Total (GBP)'] * -1

        if row['Action'] == 'Deposit':
            deposit += row['Total (GBP)']

        lst_no_of_share.append(no_of_share)
        lst_avg_price_market.append(avg_price_market)
        lst_avg_price_gbp.append(avg_price_gbp)
        lst_result_market.append(result_market)
        lst_result_gbp.append(result_gbp)
        lst_fx_impact.append(fx_impact)
        lst_dividend.append(dividend)
        lst_withdrawal.append(withdrawal)
        lst_deposit.append(deposit)

    holding['RT No. of Shares'] = lst_no_of_share
    holding['RT Avg Price Market'] = lst_avg_price_market
    holding['RT Avg Price GBP'] = lst_avg_price_gbp
    holding['RT Result Market'] = lst_result_market
    holding['RT Result GBP'] = lst_result_gbp
    holding['RT FX Impact & Fees'] = lst_fx_impact
    holding['RT Dividend'] = lst_dividend
    holding['RT Withdrawal'] = lst_withdrawal
    holding['RT Deposit'] = lst_deposit

    return holding


def data_processing(csv):
    # Convert Time into datetime format and sort the dataframe
    csv['Time'] = pd.to_datetime(csv['Time'], format='%d/%m/%Y %H:%M')
    csv = csv.sort_values(by='Time')

    # Update Action column values
    action = {'Deposit': 'Deposit',
              'Market buy': 'Buy',
              'Market sell': 'Sell',
              'Limit buy': 'Buy',
              'Limit sell': 'Sell',
              'Dividend (Ordinary)': 'Dividend',
              'Dividend (Return of capital)': 'Dividend',
              'Withdrawal': 'Withdrawal'}
    csv['Action'] = csv['Action'].map(action)
    # Raise exception if NaN in Action column

    # Fillna on Withdrawal or Deposit
    is_deposit = csv['Action'] == 'Deposit'
    csv.loc[is_deposit, 'Ticker'] = 'Deposit'
    csv.loc[is_deposit, 'Name'] = 'Deposit'
    is_withdrawal = csv['Action'] == 'Withdrawal'
    csv.loc[is_withdrawal, 'Ticker'] = 'Withdrawal'
    csv.loc[is_withdrawal, 'Name'] = 'Withdrawal'

    # Change Exchange rate column from object to float64
    csv.loc[csv['Exchange rate'] == 'Not available', 'Exchange rate'] = 0.0
    csv['Exchange rate'] = csv['Exchange rate'].astype(float)

    # Create dictionary of tickers
    tickers = csv['Ticker'].unique().tolist()
    holdings = {ticker: csv.loc[csv['Ticker'] == ticker] for ticker in tickers}

    return holdings


def generate_summary(account_dict):
    account_summary_lst = []
    for holding in account_dict:
        account_summary_lst.append(calc_return(account_dict[holding]))
    account_calc_df = pd.concat([i for i in account_summary_lst])
    idx = account_calc_df.groupby(['Name', 'Ticker'])['Time'].transform(max) == account_calc_df['Time']
    summary_cols = ['Ticker',
                    'Name',
                    'Currency (Price / share)',
                    'RT No. of Shares',
                    'RT Avg Price Market',
                    'RT Avg Price GBP',
                    'RT Result Market',
                    'RT Result GBP',
                    'RT FX Impact & Fees',
                    'RT Dividend',
                    'RT Withdrawal',
                    'RT Deposit']
    account_summary_df = account_calc_df[idx][summary_cols]

    return account_summary_df, account_calc_df


if __name__ == '__main__':
    # Read csvs
    transactions = pd.read_csv('sample_transaction_data_github.csv')
    # csv2 = pd.read_csv()

    # Merge csvs if you have more than 1 csv
    # csv = pd.concat([csv1, csv2])

    # Processed data
    account_dict = data_processing(transactions)

    # Calculations
    account_summary_df, account_calc_df = generate_summary(account_dict)

    # Exporting data frames to XLSX
    with pd.ExcelWriter('Trading212_Summary_Report_GitHubx.xlsx') as writer:
        account_summary_df.to_excel(writer, sheet_name='DUMMY ACCOUNT', index=False)

    print('Done')
