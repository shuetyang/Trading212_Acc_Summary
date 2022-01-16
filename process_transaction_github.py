import pandas as pd

class Runner:
    def __init__(
            self,
            avg_price_gbp = 0.0,
            avg_price_market = 0.0,
            result_gbp = 0.0,
            result_market = 0.0,
            fx_impact = 0.0,
            dividend = 0.0,
            deposit = 0.0,
            withdrawal = 0.0,
            no_of_share = 0.0,
            accum_cost_gbp = 0.0,
            accum_cost_market = 0.0,
        
            # New columns to be added
            lst_no_of_share = list(),
            lst_avg_price_market = list(),
            lst_avg_price_gbp = list(),
            lst_result_market = list(),
            lst_result_gbp = list(),
            lst_fx_impact = list(),
            lst_dividend = list(),
            lst_withdrawal = list(),
            lst_deposit = list()
            ):
        
        self.avg_price_gbp = avg_price_gbp
        self.avg_price_market = avg_price_market
        self.result_gbp = result_gbp
        self.result_market = result_market
        self.fx_impact = fx_impact
        self.dividend = dividend
        self.deposit = deposit
        self.withdrawal = withdrawal
        self.no_of_share = no_of_share
        self.accum_cost_gbp = accum_cost_gbp
        self.accum_cost_market = accum_cost_market
        self.lst_no_of_share = lst_no_of_share
        self.lst_avg_price_market = lst_avg_price_market
        self.lst_avg_price_gbp = lst_avg_price_gbp
        self.lst_result_market = lst_result_market
        self.lst_result_gbp = lst_result_gbp
        self.lst_fx_impact = lst_fx_impact
        self.lst_dividend = lst_dividend
        self.lst_withdrawal = lst_withdrawal
        self.lst_deposit = lst_deposit

    def data_processing(transactions):
        # Convert Time into datetime format and sort the dataframe
        transactions['Time'] = pd.to_datetime(transactions['Time'], format='%d/%m/%Y %H:%M')
        csv = transactions.sort_values(by='Time')
    
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
        account_dict = {ticker: csv.loc[csv['Ticker'] == ticker] for ticker in tickers}
    
        return account_dict
   
    def generate_summary(self,account_dict):
        account_summary_lst = []

        for holding in account_dict:
            account_summary_lst.append(self.calc_return(account_dict[holding]))
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
    
    def calc_return(self,holding):
    
        for index, row in holding.iterrows():
    
            # No of shares
            if row['Action'] == 'Buy':
                self.no_of_share += row['No. of shares']
                # Update average price after buying
                self.accum_cost_market += row['No. of shares'] * row['Price / share']
                self.accum_cost_gbp += row['No. of shares'] * row['Price / share'] / row['Exchange rate']
                self.avg_price_market = self.accum_cost_market / self.no_of_share
                self.avg_price_gbp = self.accum_cost_gbp / self.no_of_share
    
            if row['Action'] == 'Sell':
                self.result_market += (row['Price / share'] - self.avg_price_market) * row['No. of shares']
                self.result_gbp += (row['Price / share'] / row['Exchange rate'] - self.avg_price_gbp) * row['No. of shares']
                self.fx_impact += ((row['Price / share'] / row['Exchange rate'] - self.avg_price_gbp) * row['No. of shares']) - (
                        (row['Price / share'] - self.avg_price_market) / row['Exchange rate'] * row['No. of shares'])
                self.no_of_share += row['No. of shares'] * -1
                if self.no_of_share == 0:
                    self.accum_cost_gbp = 0.0
                    self.accum_cost_market = 0.0
                    self.avg_price_market = 0.0
                    self.avg_price_gbp = 0.0
    
            if row['Action'] == 'Dividend':
                self.dividend += row['Total (GBP)']
    
            if row['Action'] == 'Withdrawal':
                self.withdrawal += row['Total (GBP)'] * -1
    
            if row['Action'] == 'Deposit':
                self.deposit += row['Total (GBP)']
    
            self.lst_no_of_share.append(self.no_of_share)
            self.lst_avg_price_market.append(self.avg_price_market)
            self.lst_avg_price_gbp.append(self.avg_price_gbp)
            self.lst_result_market.append(self.result_market)
            self.lst_result_gbp.append(self.result_gbp)
            self.lst_fx_impact.append(self.fx_impact)
            self.lst_dividend.append(self.dividend)
            self.lst_withdrawal.append(self.withdrawal)
            self.lst_deposit.append(self.deposit)
    
        holding['RT No. of Shares'] = self.lst_no_of_share
        holding['RT Avg Price Market'] = self.lst_avg_price_market
        holding['RT Avg Price GBP'] = self.lst_avg_price_gbp
        holding['RT Result Market'] = self.lst_result_market
        holding['RT Result GBP'] = self.lst_result_gbp
        holding['RT FX Impact & Fees'] = self.lst_fx_impact
        holding['RT Dividend'] = self.lst_dividend
        holding['RT Withdrawal'] = self.lst_withdrawal
        holding['RT Deposit'] = self.lst_deposit
    
        return holding
   
    @staticmethod
    def export_to_xlsx(account_summary_df, sheet_name):
        # Exporting data frames to XLSX
        with pd.ExcelWriter('Trading212_Summary_Report_GitHubx.xlsx') as writer:
            account_summary_df.to_excel(writer, sheet_name='DUMMY ACCOUNT', index=False)
            
    def run(self, transactions):
        # Processed data
        account_dict = data_processing(transactions)
    
        # Calculations
        account_summary_df, account_calc_df = generate_summary(account_dict)
        
        # Save down results
        Runner.export_to_xlsx(account_summary_df, 'DUMMY ACCOUNT')

if __name__ == '__main__':
    # Read csvs
    TRANSACTIONS = pd.read_csv('sample_transaction_data_github.csv')
    
    runner = Runner()
    runner.run(TRANSACTIONS) 