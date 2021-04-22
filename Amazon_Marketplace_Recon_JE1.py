import pandas as pd
import numpy as np
from tqdm import tqdm
from Amazon_B2C_Reconciler_App import *#recon_path, recon_path_data, recon_period_market, recon_country, recon_region, path_parent
pd.options.display.float_format = '{:,.2f}'.format
print('> WRITING JOURNAL ENTRY 1 <')

if recon_country == 'US' :
    from Amazon_Order_Payment_Recon_US import *
else :
    from Amazon_Order_Payment_Recon_EMEA_CA import *

cols = ['External ID','Account','Internal ID','DR','CR','Name','Memo','Department','Product','Brand','Technology','Channel','Region']
journal_entry1 = pd.DataFrame(columns=cols,dtype=object)
je1_default_memo = 'Record Amazon Payment - '+recon_period_market

white_levies = []
red_delta_levies = []
recon_length = len(amazon_netsuite_recon_summary)
for key,val in tqdm(amazon_netsuite_recon_summary.iterrows(),total=recon_length,desc='Writing Journal Entry 1') :
    cust_name = key[0]
    cust_internal_id = key[2]
    order_id = str(key[3])
    order_sale = round(val.loc['Sale'],2)
    order_credit = round(val.loc['Credit'],2)
    order_delta = round(val.loc['Delta'],2)
    if order_delta == 0 :
        if order_sale + order_credit != 0 :
            if order_sale != 0 :
                journal_line = {'Account':'138010 - Marketplaces Transit',
                'Internal ID':'241',
                'DR':0,
                'CR':abs(order_sale),
                'Name':cust_internal_id,
                'Memo':je1_default_memo+' '+order_id}
                journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
            if order_credit != 0 :
                journal_line = {'Account':'138900 - Undeposited Fund',
                'Internal ID':'117',
                'DR':abs(order_credit),
                'CR':0,
                'Name':cust_internal_id,
                'Memo':je1_default_memo+' '+order_id}
                journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
        if order_sale + order_credit == 0 :
            journal_line = {'Account':'138010 - Marketplaces Transit',
            'Internal ID':'241',
            'DR':0,
            'CR':abs(order_sale),
            'Name':cust_internal_id,
            'Memo':je1_default_memo+' '+order_id}
            journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
            journal_line = {'Account':'138900 - Undeposited Fund',
            'Internal ID':'117',
            'DR':abs(order_credit),
            'CR':0,
            'Name':cust_internal_id,
            'Memo':je1_default_memo+' '+order_id}
            journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
    elif -20 <= order_delta <= 20 :
        white_levies = np.append(white_levies,order_delta)
        if order_sale != 0 :
            journal_line = {'Account':'138010 - Marketplaces Transit',
            'Internal ID':'241',
            'DR':0,
            'CR':abs(order_sale),
            'Name':cust_internal_id,
            'Memo':je1_default_memo+' '+order_id}
            journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
        if order_credit != 0 :
            journal_line = {'Account':'138900 - Undeposited Fund',
            'Internal ID':'117',
            'DR':abs(order_credit),
            'CR':0,
            'Name':cust_internal_id,
            'Memo':je1_default_memo+' '+order_id}
            journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
    else : # RMAs / red deltas
        red_delta_levies = np.append(red_delta_levies,order_delta)
        if order_sale + order_credit != 0 :
            if order_sale != 0 :
                journal_line = {'Account':'138010 - Marketplaces Transit',
                'Internal ID':'241',
                'DR':0,
                'CR':abs(order_sale),
                'Name':cust_internal_id,
                'Memo':je1_default_memo+' - '+order_id}
                journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
            if order_credit != 0 :
                journal_line = {'Account':'138900 - Undeposited Fund',
                'Internal ID':'117',
                'DR':abs(order_credit),
                'CR':0,
                'Name':cust_internal_id,
                'Memo':je1_default_memo+' - '+order_id}
                journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
        else :
            if order_sale != 0 :
                journal_line = {'Account':'138010 - Marketplaces Transit',
                'Internal ID':'241',
                'DR':0,
                'CR':abs(order_sale),
                'Name':cust_internal_id,
                'Memo':je1_default_memo+' - '+order_id}
                journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
            if order_credit != 0 :
                journal_line = {'Account':'138900 - Undeposited Fund',
                'Internal ID':'117',
                'DR':abs(order_credit),
                'CR':0,
                'Name':cust_internal_id,
                'Memo':je1_default_memo+' - '+order_id}
                journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
            continue

if recon_country in ['US','CA'] :
    account_internal_id = '54'
    account_name = '410101 - Revenue'
else :
    account_internal_id = '599'
    account_name = '840203 - Other Levies'

try :
    levies_sum = round(white_levies.sum(),2)
    if levies_sum < 0 :
        journal_line = {'Account':account_name,
        'Internal ID':account_internal_id,
        'DR':abs(levies_sum),
        'CR':0,
        'Name':'',
        'Memo':'White Delta Levy'}
        journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
    else :
        journal_line = {'Account':account_name,
        'Internal ID':account_internal_id,
        'DR':0,
        'CR':abs(levies_sum),
        'Name':'',
        'Memo':'White Delta Levy'}
        journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
except :
    print('No levies for Journal')

try :
    levies_rd_sum = round(red_delta_levies.sum(),2)
    # print('levies_rd_sum',levies_rd_sum)
    # false_deltas_df = amazon_netsuite_recon_summary[amazon_netsuite_recon_summary['Comments'].notna()]
    # false_deltas = false_deltas_df['Delta'].sum()
    # print('false_deltas',false_deltas)
    # levies_rd_sum = levies_rd_sum - false_deltas
    # print('levies_rd_sum',levies_rd_sum)
    print(len(red_delta_levies),'red delta levies found:',red_delta_levies,)
    if levies_rd_sum < 0 :
        journal_line = {'Account':account_name,
        'Internal ID':account_internal_id,
        'DR':abs(levies_rd_sum),
        'CR':0,
        'Name':'',
        'Memo':'Red Delta Levy'}
        journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
    else :
        journal_line = {'Account':account_name,
        'Internal ID':account_internal_id,
        'DR':0,
        'CR':abs(levies_rd_sum),
        'Name':'',
        'Memo':'Red Delta Levy'}
        journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
except :
    print('No levies for Red Deltas Journal')

try :
    sales_tax = round(amazon_netsuite_recon_summary['TAX Adjust'].sum(),2)
    journal_line = {'Account':'362003 - Sales Tax Payable DE',
    'Internal ID':'474',
    'DR':abs(sales_tax),
    'CR':0,'Name':'',
    'Memo':je1_default_memo}
    journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
except :
    print('No Sales Tax to remove from accounts receivable.')

recon_amount_1 = round(journal_entry1['CR'].sum() - journal_entry1['DR'].sum(),2)
journal_line = {'Account':'137002 - Un-reconciled B2C Payments',
'Internal ID':'742',
'DR':abs(recon_amount_1),
'CR':0,
'Name':'',
'Memo':je1_default_memo}
journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)

print('journal_entry1:',len(journal_entry1),'rows.')
