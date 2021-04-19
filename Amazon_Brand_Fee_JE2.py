import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from Amazon_B2C_Reconciler_App import *#recon_path, recon_path_data, recon_period_market, recon_country, recon_region, path_parent
from Amazon_Brand_Fee_Recon import *
from Amazon_Marketplace_Recon_JE1 import *

pd.options.display.float_format = '{:,.2f}'.format
print('> WRITING JOURNAL ENTRY 2 <')

if recon_country == 'US' :
    from Amazon_Order_Payment_Recon_US import *
else :
    from Amazon_Order_Payment_Recon_EMEA_CA import *

cols = ['External ID','Account','Internal ID','DR','CR','Name','Memo','Department','Product','Brand','Technology','Channel','Region']
journal_entry2 = pd.DataFrame(columns=cols,dtype=object)

#### Fees by Brand ####
recon_length = len(brand_commission_fulfillment_fees_summary)
for brand,fee in tqdm(brand_commission_fulfillment_fees_summary.iterrows(),total=recon_length,desc='Writing Journal Entry 2') :
    commission = fee[0]
    fulfillment = fee[1]
    if brand in ['Pjur','We-Vibe','Womanizer','Multi-Brand','Arcwave'] :
        journal_line_commission = {'Account':'640101 - Amazon Fees',
        'Internal ID':'525',
        'DR':round(abs(commission),2),
        'CR':0,'Name':'',
        'Memo':'Commission - '+recon_period_market,
        'Department':'12','Product':'Corporate',
        'Brand':brand,
        'Technology':'Corporate'}
        journal_line_fulfillment = {'Account':'610102 - Amazon Freight',
        'Internal ID':'512',
        'DR':round(abs(fulfillment),2),
        'CR':0,'Name':'',
        'Memo':'FBAPerUnitFulfillmentFee - '+recon_period_market,
        'Department':'12',
        'Product':'Corporate',
        'Brand':brand,
        'Technology':'Corporate'}
        journal_entry2 = journal_entry2.append([journal_line_commission,journal_line_fulfillment],ignore_index=True)
    elif brand in ['Romp','Realm'] :
        journal_line_commission = {'Account':'640101 - Amazon Fees',
        'Internal ID':'526',
        'DR':round(abs(commission),2),
        'CR':0,'Name':'',
        'Memo':'Commission - '+recon_period_market,
        'Department':'12',
        'Product':'Corporate',
        'Brand':brand,
        'Technology':'Corporate'}
        journal_line_fulfillment = {'Account':'610102 - Amazon Freight',
        'Internal ID':'512',
        'DR':round(abs(fulfillment),2),
        'CR':0,
        'Name':'',
        'Memo':'FBAPerUnitFulfillmentFee - '+recon_period_market,
        'Department':'12',
        'Product':'Corporate',
        'Brand':brand,
        'Technology':'Corporate'}
        journal_entry2 = journal_entry2.append([journal_line_commission,journal_line_fulfillment],ignore_index=True)
    else :
        continue

### Breakdown of Compensated Clawback and Reversal Reimbursement by Brand and Customer ###
for key,val in clawback_reimbursement_breakdown.iterrows() :
    amount = val['amount']
    cust_name = val['Name']
    order_id = str(val['order-id'])
    try :
        cust_internal_id = val['Internal ID_x']
    except:
        cust_internal_id = val['Internal ID']
    product = val['PRODUCT']
    brand = val['BRAND']
    technology = val['PRODUCT TYPE']
    amount_description = val['amount-description']
    if amount_description == 'COMPENSATED_CLAWBACK' :
        journal_line = {'Account':'410101 - Revenue',
        'Internal ID':54,
        'DR':round(abs(amount),2),
        'CR':0,
        'Name':cust_internal_id,
        'Memo':amount_description+' - '+recon_period_market+' - '+order_id,
        'Department':'12',
        'Product':product,
        'Brand':brand,
        'Technology':technology}
    else :
        journal_line = {'Account':'410101 - Revenue',
        'Internal ID':54,
        'DR':0,
        'CR':round(abs(amount),2),
        'Name':cust_internal_id,
        'Memo':amount_description+' - '+recon_period_market+' - '+order_id,
        'Department':'12',
        'Product':product,
        'Brand':brand,
        'Technology':technology}
    journal_entry2 = journal_entry2.append(journal_line,ignore_index=True)

#### Fees charged to Corporate ####
for key,val in amount_type_fees_summary.iterrows() :
    amount_description = key[2]
    amount_sum = val['amount']
    if amount_description in ['FBAPerUnitFulfillmentFee','Commission','COMPENSATED_CLAWBACK','REVERSAL_REIMBURSEMENT'] :#(exclusion list)#,'Previous Reserve Amount Balance','Current Reserve Amount','Previous Reserve Amount Balance','Current Reserve Amount'
        continue
    for type,(id,account) in amount_description_types.items() :
        if amount_description == type :
            if amount_sum < 0 :
                journal_line = {'Account':account,
                'Internal ID':id,
                'DR':round(abs(amount_sum),2),
                'CR':0,
                'Name':'',
                'Memo':type+' - '+str(recon_period_market),
                'Department':'12',
                'Product':'Corporate',
                'Brand':'Corporate',
                'Technology':'Corporate'}
            else :
                journal_line = {'Account':account,
                'Internal ID':id,
                'DR':0,
                'CR':round(abs(amount_sum),2),
                'Name':'',
                'Memo':type+' - '+str(recon_period_market),
                'Department':'12',
                'Product':'Corporate',
                'Brand':'Corporate',
                'Technology':'Corporate'}
            journal_entry2 = journal_entry2.append(journal_line,ignore_index=True)

#### Credit to Account "Unreconciled B2C Payments" ####
recon_amount_2 = round(abs(sum(journal_entry2['CR'] - journal_entry2['DR'])),2)
journal_line = {'Account':'137002 - Un-reconciled B2C Payments',
'Internal ID':742,
'DR':0,
'CR':recon_amount_2,
'Name':'',
'Memo':'Bank Deposit - '+recon_period_market,
'Department':'12',
'Product':'Corporate',
'Brand':'Corporate',
'Technology':'Corporate'}
journal_entry2 = journal_entry2.append(journal_line,ignore_index=True)
journal_entry2['External ID'],journal_entry2['Channel'],journal_entry2['Region'] = recon_period_market+r'\JE2','B2C : Mainstream Amazon',recon_region
journal_entry2 = journal_entry2.set_index('External ID')

import Amazon_RMAs_JE3
