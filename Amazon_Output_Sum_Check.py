import pandas as pd
import os
pd.options.display.float_format = '{:,.2f}'.format

from Amazon_Brand_Fee_Recon import *
from Amazon_Marketplace_Recon_JE1 import *
from Amazon_Brand_Fee_JE2 import *
from Amazon_Data_Cleansing import *


if recon_country == 'US' :
    from Amazon_Order_Payment_Recon_US import *
else :
    from Amazon_Order_Payment_Recon_EMEA_CA import *

red_deltas_df = amazon_netsuite_recon_summary[(amazon_netsuite_recon_summary['Delta'] > 20) | (amazon_netsuite_recon_summary['Delta'] < -20)]
red_deltas_df.to_csv(recon_path_data+r'\red_deltas_df.csv')
red_deltas_df['Comments'] = ''
if len(red_deltas_df) != 0 :
    if len(red_deltas_df) > 0 :
        for key,val in red_deltas_df.iterrows() :
            order_id = key[2]
            if val['Grand Total'] >= 0 and val['Delta'] == val['Grand Total'] and (val['Sale'] and val['Credit']) == 0 :
                red_deltas_df['Comments'].loc[key] = 'Missing Payment'
            elif abs(val['Delta']) < abs(val['Grand Total']) :
                red_deltas_df['Comments'].loc[key] = 'Base Price Delta'
            # elif input_missing_orderids == 'y' and (val['Sale'] != 0) and (abs(val['Credit']) > val['Sale']) :
            #     red_deltas_df['Comments'].loc[key] = 'Refunds summed up incorrectly due to missing order-ids (match via cus_id)'
            elif (val['Credit'] != 0) and (val['Sale'] > abs(val['Credit'])) :
                red_deltas_df['Comments'].loc[key] = 'Refund less than total amount'
            elif abs(val['Delta']) / 2 == abs(val['Grand Total']) or val['Grand Total'] == 0 or ( val['Grand Total'] < 0 and val['Credit'] == 0 ) :
                red_deltas_df['Comments'].loc[key] = 'Missing Refund'
            elif val['Grand Total'] < 0 and val['Principal'] == val['Grand Total'] and val['Sale'] == abs(val['Credit']) :
                red_deltas_df['Comments'].loc[key] = r"Purchase in previous period's Amazon Statement"
            else :
                red_deltas_df['Comments'].loc[key] = 'Unknown Error: Check Delta'

# try :
so_missing_rma = red_deltas_df[red_deltas_df['Comments'] == 'Missing Refund']
if len(so_missing_rma) > 0 :
    so_missing_rma = [key[3] for key,val in so_missing_rma.iterrows()]
    print(so_missing_rma)
    netsuite_orders_rma = pd.read_csv(path_parent+r'\NetSuite Data\NetSuite_Amazon_orders.csv', delimiter=',',dtype=object,encoding='iso8859_15').rename(columns={'ChannelAdvisor Order ID':'order-id',}) #,usecols=['ChannelAdvisor Order ID','Name']

    netsuite_orders_rma = netsuite_orders_rma[netsuite_orders_rma['order-id'].isin(so_missing_rma)]
    netsuite_orders_rma = netsuite_orders_rma.iloc[:,[0,3,4,7]]

    try :
        netsuite_orders_rma = netsuite_orders_rma[netsuite_orders_rma['Item'].str.startswith('PRD')].sort_values('Internal ID')
        netsuite_orders_rma['No. of Line Items'] = netsuite_orders_rma.groupby(['Internal ID'])['Item'].transform('count')
        print(netsuite_orders_rma)
    except Exception as e :
        print('netsuite_orders_rma: No Items starting with PRD in the column:',e)


    rmas_pending_receipt = []
    for key,val in netsuite_orders_rma.iterrows() :
        if val['Type'] == 'Credit Memo' :
            rmas_pending_receipt.append(val['order-id'])

    rmas_pending_receipt = set(rmas_pending_receipt)

    for key,val in netsuite_orders_rma.iterrows() :
        for i in rmas_pending_receipt :
            if i == val['order-id'] :
                # print(i)
                netsuite_orders_rma = netsuite_orders_rma.drop(key)
else :
    print(r'"/!\ len(so_missing_rma) == 0 "')
# except :
#     print('so_missing_rma not generated')

#### Recon Summary ####
os.chdir(recon_path+r'\Output Files')
with pd.ExcelWriter('Recon Summary'+recon_country+'.xlsx') as recon_summary :
    amazon_netsuite_recon_summary.to_excel(recon_summary,sheet_name='Recon Summary')
    if len(red_deltas_df) != 0 :
        if len(red_deltas_df) > 0 :
            for key,val in red_deltas_df.iterrows() :
                order_id = key[2]
                if len(so_missing_rma) > 0 :
                    if order_id in rmas_pending_receipt :
                        red_deltas_df['Comments'].loc[key] = 'RMA Missing Refund / Pending Receipt'
                elif abs(val['Delta']) / 2 == abs(val['Grand Total']) or val['Grand Total'] == 0 :
                    red_deltas_df['Comments'].loc[key] = 'Missing RMA'
                elif val['Grand Total'] >= 0 and val['Delta'] == val['Grand Total'] and (val['Sale'] and val['Credit']) == 0 :
                    red_deltas_df['Comments'].loc[key] = 'Missing Payment'
                elif abs(val['Delta']) < abs(val['Grand Total']) :
                    red_deltas_df['Comments'].loc[key] = 'Base Price Delta'
                else :
                    red_deltas_df['Comments'].loc[key] = 'Unknown Error: Check Delta'
        red_deltas_df.to_excel(recon_summary,sheet_name='Red Deltas Report')
    else :
        print('No Red Deltas.')
    if len(principals_missing_internalids) != 0 :
        principals_missing_internalids.to_excel(recon_summary,sheet_name='Principals Missing Internal IDs')
    else :
        print('No principals missing Internal IDs.')
    amount_type_fees_summary.to_excel(recon_summary,sheet_name='Overall Fees Summary')
    brand_commission_fulfillment_fees_summary.to_excel(recon_summary,sheet_name='Brand Comm. & Fulfil. Fees')

print('> JOURNAL ENTRY 1 & 2 SUM-CHECK vs AMAZON STATEMENT <')

##### JE1,JE2,JETotal #####
amount_type_fees_summary_amount_type = round(amount_type_fees_summary.groupby('amount-type').sum(),2)
recon_target = round(amount_type_fees_summary_amount_type.loc['Grand Total']['amount'],2)
try :
    recon_amount_1 = round(amount_type_fees_summary_amount_type.loc['ItemPrice']['amount'] + amount_type_fees_summary_amount_type.loc['Promotion']['amount'],2)
except Exception as e :
    print(e)
    recon_amount_1 = round(amount_type_fees_summary_amount_type.loc['ItemPrice']['amount'])
recon_amount_2 = abs(round(recon_target - recon_amount_1,2))
###############

je1_amount = round(journal_entry1.loc[journal_entry1['Account'] == '137002 - Un-reconciled B2C Payments','DR'].values[0],2)
je2_amount = round(journal_entry2.loc[journal_entry2['Account'] == '137002 - Un-reconciled B2C Payments','CR'].values[0],2)

print('Journal Entry 1:',je1_amount,'|| Journal Entry 1 Target:',recon_amount_1,'|| Difference:', round(je1_amount - recon_amount_1,2))
print('Journal Entry 2:',je2_amount,'|| Journal Entry 2 Target:',recon_amount_2,'|| Difference:', round(je2_amount - recon_amount_2,2))
recon_sum_check = round(je1_amount - je2_amount - recon_target,2)
print('> Recon Sum-Check <')
print('Journal Entry 1:',je1_amount,'|| Journal Entry 2:',je2_amount,'|| Amazon Statement:',recon_target,'|| Sum-Check:',recon_sum_check)

correction_check = input('> Do you want to correct the Sum-Check difference? < (y/n) : ')

if correction_check == 'y' :
    if recon_sum_check < 0 :
        journal_line = {'Account':'840203 - Other Levies',
        'Internal ID':'599',
        'DR':0,
        'CR':abs(recon_sum_check),
        'Name':'',
        'Memo':'Recon Sum-Check Levy'}
    else :
        journal_line = {'Account':'840203 - Other Levies',
        'Internal ID':'599',
        'DR':abs(recon_sum_check),
        'CR':0,
        'Name':'',
        'Memo':'Recon Sum-Check Levy'}

    journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)

    journal_entry1.loc[journal_entry1['Account'] == '137002 - Un-reconciled B2C Payments','DR'] = 0
    unrec_payments_1 = round(abs(journal_entry1['CR'].sum() - journal_entry1['DR'].sum()),2)
    journal_entry1.loc[journal_entry1['Account'] == '137002 - Un-reconciled B2C Payments','DR'] = unrec_payments_1

journal_entry1['External ID'],journal_entry1['Department'],journal_entry1['Product'],journal_entry1['Brand'],journal_entry1['Technology'],journal_entry1['Channel'],journal_entry1['Region'] = recon_period_market,'12','Corporate','Corporate','Corporate','B2C : Mainstream Amazon',recon_region
journal_entry1 = journal_entry1.set_index('External ID').sort_values('Internal ID')

journal_entry1.to_csv(recon_path+r'\Output Files\journal_entry1'+recon_country+'.csv')
journal_entry2.to_csv(recon_path+r'\Output Files\journal_entry2'+recon_country+'.csv')

print('journal_entry2:',len(journal_entry2),'rows.')
recon_sum_check = round(je1_amount - je2_amount - recon_target,2)
print('> Recon Sum-Check <')
print('Journal Entry 1:',je1_amount,'|| Journal Entry 2:',je2_amount,'|| Amazon Statement:',recon_target,'|| Sum-Check:',recon_sum_check)

if recon_country == 'US' :
    fee_comparison = amount_type_fees_summary.reset_index()
    fee_comparison = fee_comparison['amount-description']
    missing_fees = set(fee_comparison) - set(amount_description_types)
    print('Following fees could be missing for the US:',missing_fees)

print('>> RECONCILIATION COMPLETED <<')

import Amazon_RMAs_JE3
