import pandas as pd
import os
pd.options.display.float_format = '{:,.2f}'.format

from Amazon_Output_Sum_Check import *
from Amazon_Marketplace_Recon_JE1 import *

try :
    manual_journal_entry3 = netsuite_orders_rma[netsuite_orders_rma['No. of Line Items'] == 1]
    manual_journal_entry3 = netsuite_orders_rma.merge(amazon_netsuite_recon_summary,on='order-id',how='left')
    manual_journal_entry3 = manual_journal_entry3[['order-id','Grand Total', 'Sale', 'Credit', 'Delta']].rename(columns={'order-id':'ChannelAdvisor oder-id','Grand Total':'Amazon Statement','Sale':'NS Payment','Credit':'NS Refund'}).drop_duplicates('ChannelAdvisor oder-id')
    manual_journal_entry3.to_csv(recon_path+r'\Output Files\manual_journal_entry3.csv')
except Exception as e :
    print(r'/!\ REPORT FAILED: manual_journal_entry3 /!\','"Error: ", e)

try :
    journal_entry3 = netsuite_orders_rma[netsuite_orders_rma['No. of Line Items'] == 1].drop(['Type','order-id','Item','No. of Line Items'],1)
    journal_entry3['FBA Refund'] = 'T'
    journal_entry3 = journal_entry3.rename(columns={'Internal ID':'Sales Order : Internal ID'}).set_index('Sales Order : Internal ID')#.reset_index(drop=True)
    journal_entry3.to_csv(recon_path+r'\Output Files\journal_entry3'+recon_country+'.csv')
except Exception as e :
    print(r'/!\ REPORT FAILED: journal_entry3 /!\','"Error: ", e)
# import Amazon_Output_Sum_Check
