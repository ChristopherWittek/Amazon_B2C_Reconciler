import pandas as pd
import os
from tqdm import tqdm
from Amazon_B2C_Reconciler_App import *
from Amazon_Data_Cleansing import *
pd.options.display.float_format = '{:,.2f}'.format
print('> PROCESSING RECONCILIATION: EMEA & CA <')

# create table with only relevant amazon seller center amount descriptions
recon_amount_descriptions_criteria = amazon_statement_with_cust_name[
(amazon_statement_with_cust_name['amount-description'] == 'Principal') |
(amazon_statement_with_cust_name['amount-description'] == 'Shipping') |
(amazon_statement_with_cust_name['amount-description'] == 'Tax') |
(amazon_statement_with_cust_name['amount-description'] == 'ShippingTax') |
(amazon_statement_with_cust_name['amount-description'] == 'GiftWrap') |
(amazon_statement_with_cust_name['amount-description'] == 'GiftWrapTax') |
(amazon_statement_with_cust_name['amount-description'] == 'Goodwill')]
recon_amount_descriptions_criteria = recon_amount_descriptions_criteria[
(recon_amount_descriptions_criteria.index != 'order-id') &
 (recon_amount_descriptions_criteria.index.notnull())]
recon_amount_descriptions_criteria['Name'] = recon_amount_descriptions_criteria['Name'].fillna('CUSTOMER_NAME_MISSING')
recon_amount_descriptions_criteria['amount'] = recon_amount_descriptions_criteria['amount'].astype(float)

# create pivot table with amount descriptions as columns
amazon_netsuite_recon_summary = pd.pivot_table(recon_amount_descriptions_criteria,
index=['Name','ID','Internal ID','order-id'],
values='amount',
columns='amount-description',
aggfunc='sum').fillna(0).astype(float)
amazon_netsuite_recon_summary['Grand Total'] = amazon_netsuite_recon_summary.sum(axis=1)

amazon_netsuite_recon_summary['Sale'] = 0.0
amazon_netsuite_recon_summary['Credit'] = 0.0
amazon_netsuite_recon_summary['Delta'] = 0.0
amazon_netsuite_recon_summary['Comments'] = ''
recon_length = len(amazon_netsuite_recon_summary)
for key,val in tqdm(amazon_netsuite_recon_summary.iterrows(),total=recon_length,desc='Reconciling payments/refunds using order-id') :
    cust_name = key[0]
    cus_id = key[1]
    order_id = key[3]
    principal = val.loc['Principal']
    grand_total = val.loc['Grand Total']
    sale = val.loc['Sale']
    credit = val.loc['Credit']
    delta = val.loc['Delta']
    if principal == 0 :
        amazon_netsuite_recon_summary['Sale'].loc[key] = netsuite_payments.loc[(netsuite_payments.index == order_id) & (netsuite_payments['Type'] == 'Payment')]['Amount (Foreign Currency)'].astype(float).sum()
        amazon_netsuite_recon_summary['Credit'].loc[key] = netsuite_payments.loc[(netsuite_payments.index == order_id) & (netsuite_payments['Type'] == 'Customer Refund')]['Amount (Foreign Currency)'].astype(float).sum()
    # if ( sale != 0 and credit != 0 ) and ( sale == abs(credit) ) and principal < 0 :# and ( principal == grand_total )  and principal <
    #     print(principal,sale,credit,sale+credit)
    #     amazon_netsuite_recon_summary['Comments'].loc[key] = r"Purchase in previous period's Amazon Statement (excluded from red delta levy)"
    elif principal < 0 :
        amazon_netsuite_recon_summary['Credit'].loc[key] = netsuite_payments.loc[(netsuite_payments.index == order_id) & (netsuite_payments['Type'] == 'Customer Refund')]['Amount (Foreign Currency)'].astype(float).sum()
    elif principal > 0 :
        amazon_netsuite_recon_summary['Sale'].loc[key] = netsuite_payments.loc[(netsuite_payments.index == order_id) & (netsuite_payments['Type'] == 'Payment')]['Amount (Foreign Currency)'].astype(float).sum()

    amazon_netsuite_recon_summary['Delta'].loc[key] = amazon_netsuite_recon_summary['Grand Total'].loc[key] - amazon_netsuite_recon_summary['Sale'].loc[key] - amazon_netsuite_recon_summary['Credit'].loc[key]

    if delta > 20 or delta < -20 : # ensure the red_deltas are actual ones and not the result of payments or refunds not picked up by conditions above
        amazon_netsuite_recon_summary['Sale'].loc[key] = netsuite_payments.loc[(netsuite_payments.index == order_id) & (netsuite_payments['Type'] == 'Payment')]['Amount (Foreign Currency)'].astype(float).sum()
        amazon_netsuite_recon_summary['Credit'].loc[key] = netsuite_payments.loc[(netsuite_payments.index == order_id) & (netsuite_payments['Type'] == 'Customer Refund')]['Amount (Foreign Currency)'].astype(float).sum()

    amazon_netsuite_recon_summary['Delta'].loc[key] = amazon_netsuite_recon_summary['Grand Total'].loc[key] - amazon_netsuite_recon_summary['Sale'].loc[key] - amazon_netsuite_recon_summary['Credit'].loc[key]


# second iteration here to match refunds that are missing order-ids (didn't work in the loop above so had to separate)
# ensure the red_deltas are actual and not because the refunds have 'Amazon' in the memo instead of the order-id
# if input_missing_orderids == 'y' :
#     for key,val in tqdm(amazon_netsuite_recon_summary.iterrows(),total=recon_length,desc='Reconciling payments/refunds using Customer Name') :
#         cust_name = key[0]
#         cus_id = key[1]
#         order_id = key[3]
#         principal = val.loc['Principal']
#         grand_total = val.loc['Grand Total']
#         sale = val.loc['Sale']
#         credit = val.loc['Credit']
#         delta = val.loc['Delta']
#         if delta < 0 and abs(delta) == sale and grand_total == 0 :#1
#             amazon_netsuite_recon_summary['Credit'].loc[key] = netsuite_payments.loc[(netsuite_payments['ID'] == cus_id) & (netsuite_payments['Type'] == 'Customer Refund') & ('Amazon' in netsuite_payments.index)]['Amount (Foreign Currency)'].astype(float).sum()#(bool(re.search('^[0-9-]*$',str(netsuite_payments.index)) == False))
#         if delta < 0 and abs(delta) / 2 == grand_total :#2
#             amazon_netsuite_recon_summary['Credit'].loc[key] = netsuite_payments.loc[(netsuite_payments['ID'] == cus_id) & (netsuite_payments['Type'] == 'Customer Refund') & ('Amazon' in netsuite_payments.index)]['Amount (Foreign Currency)'].astype(float).sum()#'Amazon' in netsuite_payments.index
#         amazon_netsuite_recon_summary['Delta'].loc[key] = amazon_netsuite_recon_summary['Grand Total'].loc[key] - amazon_netsuite_recon_summary['Sale'].loc[key] - amazon_netsuite_recon_summary['Credit'].loc[key]
#
# amazon_netsuite_recon_summary = amazon_netsuite_recon_summary#.astype(float)

# amazon_netsuite_recon_summary.to_csv(recon_path_data+r'\amazon_netsuite_recon_summary.csv')
