import pandas as pd
import os
pd.options.display.float_format = '{:,.2f}'.format

from Amazon_B2C_Reconciler_App import *
from Amazon_Brand_Fee_Recon import *

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

# from Amazon_Output_Sum_Check import *
from Amazon_Marketplace_Recon_JE1 import *

try :
    manual_rma_entry = netsuite_orders_rma[netsuite_orders_rma['No. of Line Items'] == 1]
    manual_rma_entry = netsuite_orders_rma.merge(amazon_netsuite_recon_summary,on='order-id',how='left')
    manual_rma_entry = manual_rma_entry[['order-id','Grand Total', 'Sale', 'Credit', 'Delta']].rename(columns={'order-id':'ChannelAdvisor oder-id','Grand Total':'Amazon Statement','Sale':'NS Payment','Credit':'NS Refund'}).drop_duplicates('ChannelAdvisor oder-id')
    manual_rma_entry.to_csv(recon_path+r'\Output Files\manual_RMA_entry'+recon_country+'.csv')
except Exception as e :
    print(r'/!\ REPORT FAILED: manual_rma_entry /!\','"Error: ", e)

try :
    rma_csv_upload = netsuite_orders_rma[netsuite_orders_rma['No. of Line Items'] == 1].drop(['Type','order-id','Item','No. of Line Items'],1)
    rma_csv_upload['FBA Refund'] = 'T'
    rma_csv_upload = rma_csv_upload.rename(columns={'Internal ID':'Sales Order : Internal ID'}).set_index('Sales Order : Internal ID')#.reset_index(drop=True)
    rma_csv_upload.to_csv(recon_path+r'\Output Files\RMA_CSV_upload'+recon_country+'.csv')
except Exception as e :
    print(r'/!\ REPORT FAILED: rma_csv_upload /!\','"Error: ", e)


import Amazon_Output_Sum_Check
