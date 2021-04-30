import pandas as pd
import numpy as np
import os
import glob
import re
from tqdm import tqdm
from Amazon_B2C_Reconciler_App import * #recon_path, recon_path_data, recon_period_market, recon_country, recon_region, path_parent
pd.options.mode.chained_assignment = None # supress chained_assignment warnings
pd.options.display.float_format = '{:,.2f}'.format # format floats to two decimal-places
print('> EXTRACTING AND CLEANSING DATA <')

os.chdir(recon_path_data)# change directory to file path where data is
amazon_txt_statements = glob.glob('./*.txt')# list all files with .txt suffixes
txt_statements_opt = input('> Create new amazon_seller_center_report using text-statements? < (y/n) : ')
if txt_statements_opt == 'y' :
    amazon_seller_center_report = pd.DataFrame()# create empty dataframe
    for statement in amazon_txt_statements :
        frame = pd.read_csv(statement,delimiter='\t',decimal=',',dtype=object,encoding='iso8859_15')
        amazon_seller_center_report = amazon_seller_center_report.append(frame)
    amazon_seller_center_report = amazon_seller_center_report[amazon_seller_center_report['settlement-id'] != 'settlement-id']
    amazon_seller_center_report['amount'] = amazon_seller_center_report['amount'].str.strip().str.replace(',','.').astype(float)
    amazon_seller_center_report['total-amount'] = amazon_seller_center_report['total-amount'].str.strip().str.replace(',','.').astype(float)
    amazon_seller_center_report['clean_check'] = ''
    amazon_seller_center_report = amazon_seller_center_report.set_index('settlement-id').to_csv('Amazon_seller_center_report.csv')

amazon_seller_center_report = pd.read_csv(recon_path_data+r'\Amazon_seller_center_report.csv',delimiter=',',dtype=object,encoding='iso8859_15')
amazon_seller_center_report['total-amount'] = amazon_seller_center_report['total-amount'].astype(float)
recon_target_sum = round(amazon_seller_center_report['total-amount'].sum(),2)
currency = amazon_seller_center_report['currency'][0]
print('>> TOTAL STATEMENT PAY-OUT :',recon_target_sum,currency,'<<')

############ CLEAN MESSY AMAZON SKUs (works but extremely slow - okay for all countries except USA) ######################
netsuite_product_data = pd.read_csv(path_parent+r'\NetSuite Data\AMZ_Products.csv',decimal=',',delimiter=',',encoding='iso8859_15').rename(columns={'Feed Name':'sku'}).drop('Name',axis=1)#.set_index('sku')
### sort netsuite_product_data['sku'] by string length in order to search using the longest skus first ###
netsuite_product_data['sku_len'] = netsuite_product_data.sku.str.len().sort_values(ascending=False)
netsuite_product_data = netsuite_product_data.sort_values('sku_len',ascending=False)
netsuite_product_data['AMZ ASIN'] = netsuite_product_data['AMZ ASIN'].fillna('')

clean_len = len(netsuite_product_data)
clean_up_opt = input('> Clean up Amazon SKUs? < (y/n) : ')# cleaning skus takes about 15 minutes for US
if clean_up_opt == 'y' :
    for index1,row1 in tqdm(netsuite_product_data.iterrows(),total=clean_len) :
        netsuite_sku = str(row1['sku'])
        if row1['AMZ ASIN'] == '' :
            continue
        else :
            amz_asin = str(row1['AMZ ASIN'])
        for index2,row2 in amazon_seller_center_report.iterrows() :
            amazon_sku = str(row2['sku'])
            if bool(re.search('[.]',amazon_sku)) == True or bool(re.search('[-]',amazon_sku)) == True and re.search('.{3}$',amazon_sku).group(0) not in ['N01','-01','-02','-03','-04','-05'] : # Pjur skus are the only ones with '-' in them
                try :
                    amazon_seller_center_report['sku'].loc[index2] = re.search('[^.]+(?=-)',amazon_sku).group()#(?=-)
                    print('RegEx SKU CLEAN ||','amazon_sku:',amazon_sku,'|| cleaned_sku:',re.search('[^.]+(?=-)',amazon_sku).group())
                except :
                    amazon_seller_center_report['sku'].loc[index2] = re.search('[^.]+$',amazon_sku).group()#(?=-)
                    print('RegEx SKU CLEAN ||','amazon_sku:',amazon_sku,'|| cleaned_sku:',re.search('[^.]+$',amazon_sku).group())
            if row2['clean_check'] != None :
                if bool(re.search(netsuite_sku,amazon_sku)) == True and netsuite_sku != amazon_sku and re.search('.{3}$',amazon_sku).group(0) not in ['N01'] : # The Pjur sku with '-N01' in it gets overwritten if not added in condition
                    amazon_seller_center_report['sku'].loc[index2] = netsuite_sku
                    amazon_seller_center_report['clean_check'].loc[index2] = 'cleaned'
                    print('netsuite_sku MATCH ||','amazon_sku:',amazon_sku,'|| netsuite_sku:',netsuite_sku)
                elif bool(re.search(amz_asin,amazon_sku)) == True and netsuite_sku != amazon_sku and re.search('.{3}$',amazon_sku).group(0) not in ['N01'] : # The Pjur sku with '-N01' in it gets overwritten if not added in condition
                    amazon_seller_center_report['sku'].loc[index2] = netsuite_sku
                    amazon_seller_center_report['clean_check'].loc[index2] = 'cleaned'
                    print('amz_asin MATCH ||','amazon_sku:',amazon_sku,'|| netsuite_sku:',netsuite_sku)

    amazon_seller_center_report.to_csv('Amazon_seller_center_report.csv')
else :
    amazon_seller_center_report.to_csv('Amazon_seller_center_report.csv')

#create the orders table and remove all tax customers
netsuite_orders = pd.read_csv(path_parent+r'\NetSuite Data\NetSuite_Amazon_orders.csv', delimiter=',',dtype=object,usecols=['ChannelAdvisor Order ID','Name'],encoding='iso8859_15').rename(columns={'ChannelAdvisor Order ID':'order-id',})
netsuite_orders = netsuite_orders[
    (netsuite_orders['Name'] != 'Tax Agency IT : Tax Agency IT (WOW Tech Europe GmbH)') &
    (netsuite_orders['Name'] != 'Tax Agency DE') &
    (netsuite_orders['Name'] != 'HM Revenue & Customs : HM Revenue & Customs (WOW Tech Europe GmbH)') &
    (netsuite_orders['Name'] != 'Tax Agency AT') &
    (netsuite_orders['Name'] != 'Tax Agency BE : Tax Agency BE (WOW Tech Europe GmbH) (20191213-083848)') &
    (netsuite_orders['Name'] != 'Tax Agency CZ : Tax Agency CZ (WOW Tech Europe GmbH) (20200507-085159)') &
    (netsuite_orders['Name'] != 'Tax Agency ES : Tax Agency ES (WOW Tech Europe GmbH)') &
    (netsuite_orders['Name'] != 'Tax Agency FR') &
    (netsuite_orders['Name'] != 'Tax Agency NL : Tax Agency NL (WOW Tech Europe GmbH) (20200507-090023)') &
    (netsuite_orders['Name'] != 'Tax Agency PL : Tax Agency PL (WOW Tech Europe GmbH) (20200322-080800)') &
    (netsuite_orders['Name'] != '15V000590 Avalara Canada') &
    (netsuite_orders['Name'] != 'Minister of Finance ON') &
    (netsuite_orders['Name'] != 'Receiver General') &
    (netsuite_orders['Name'] != '15V000503 Avalara') &
    (netsuite_orders['Name'] != 'Tax Agency IE : Tax Agency IE (1 - WOW Tech Europe GmbH) (20201111-034756)')].drop_duplicates()
netsuite_orders = netsuite_orders.drop_duplicates(subset='order-id', keep="last")
# Check for Customer Names with 'Tax' in it
tax_check = []
netsuite_orders_len = len(netsuite_orders)
for order_id,name in tqdm(netsuite_orders.iterrows(),total=netsuite_orders_len,desc='Checking customer names for the word "Tax"') :
    name = str(name['Name'])
    if bool(re.search('Tax',name)) == True :
        tax_check.append(name)
    else :
        continue
print('>> Names containing the word "Tax" in netsuite_orders (for removal of Tax Debtors):',set(tax_check),'<<')

amazon_seller_center_report['order-id'] = amazon_seller_center_report['order-id'].astype(str)
netsuite_orders['order-id'] = netsuite_orders['order-id'].astype(str)
amazon_statement_with_cust_name = amazon_seller_center_report.merge(netsuite_orders,how='left',left_on='order-id',right_on='order-id')
amazon_statement_with_cust_name['ID'] = amazon_statement_with_cust_name['Name'].str.split(' ').str[0]
cust_internal_ids = pd.read_excel(path_parent+r'\NetSuite Data\Cust_Internal_IDs.xlsx',dtype=object,usecols=['ID','Internal ID'])#delimiter=',',encoding='iso8859_15',
print('Number of rows in file Cust_Internal_IDs.xlsx: ',len(cust_internal_ids),'\n')
amazon_statement_with_cust_name = amazon_statement_with_cust_name.merge(cust_internal_ids,how='left',on='ID')

principals_missing_internalids = amazon_statement_with_cust_name[(amazon_statement_with_cust_name['Internal ID'].isna()) & (amazon_statement_with_cust_name['amount-description'] == 'Principal')]
principals_missing_internalids = principals_missing_internalids[['order-id','amount-description','amount','posted-date','Name','Internal ID']]

if len(principals_missing_internalids) > 0 :
    print(r'"/!\ WARNING:',len(principals_missing_internalids),r'orders with principals missing internal IDs that will result in',principals_missing_internalids['amount'].agg(lambda x : pd.to_numeric(x,errors='coerce')).sum(),r' (without fees) Sum Check Levy /!\"')
    print('  !  LIST OF EXAMPLES: ',set(principals_missing_internalids['order-id']))
    print('  !  1) Check if order-ids exist in Netsuite\n  !  2) Check if Orders Report is in the date range of the statements\n  !  3) Check if there is an issue merging cust_internal_ids, or if there is an issue with the file\n')
    principals_missing_internalids.to_csv(recon_path_data+r'\principals_missing_internalids.csv')
else :
    print('No principals with missing internal IDs')

amazon_statement_with_cust_name.to_csv('amazon_statement_with_cust_name.csv')

netsuite_payments = pd.read_csv(path_parent+r'\NetSuite Data\NetSuite_Amazon_payments.csv',
delimiter=',',
dtype=object,
header=6,
usecols=['Name',
'Memo',
'Type',
'Amount (Foreign Currency)',
'Account (Line): Internal ID'],
encoding='iso8859_15').rename(columns={'Memo':'order-id'}).fillna('Amazon').set_index(['order-id'])#.dropna()
netsuite_payments['Amount (Foreign Currency)'] = netsuite_payments['Amount (Foreign Currency)'].str.strip().str.replace(',','')
netsuite_payments['ID'] = netsuite_payments['Name'].str.split(' ').str[0]

netsuite_payments.to_csv('netsuite_payments.csv')

netsuite_orders_tax = pd.read_csv(path_parent+r'\NetSuite Data\NetSuite_Amazon_orders.csv',delimiter=',',encoding='iso8859_15',dtype=object).rename(columns={'ChannelAdvisor Order ID':'order-id'})

def float_cleanser(dirty_series) :
    dirty_series = dirty_series.str.replace(',','').str.replace('(','-').str.replace('€','').str.strip().str.replace('$','').str.replace('£','').str.replace(r'-â\x82','').str.replace('Â','').str.rstrip(')').str.replace('Can','').str.replace('--','-').str.replace('¬','').astype(float)
    global clean_series
    clean_series = dirty_series
    return clean_series

try :
    netsuite_orders_tax['Amount (Foreign Currency)'] = float_cleanser(netsuite_orders_tax['Amount (Foreign Currency)'])
except Exception as e :
    print(e)

amount_description_types = {'Commission':(525,'640101 - Amazon Fees'),
    'COMPENSATED_CLAWBACK':(54,'410101 - Revenue'),
    'REVERSAL_REIMBURSEMENT':(54,'410101 - Revenue'),
    'WAREHOUSE_DAMAGE':(435,'630101 - Scrap'),
    'FREE_REPLACEMENT_REFUND_ITEMS':(54,'410101 - Revenue'),
    'GiftwrapChargeback':(54,'410101 - Revenue'),
    'Early Reviewer Program fee':(336,'810309 - Advertising'),
    'Storage Fee':(350,'620101 - Storage'),
    'SalesTaxServiceFee':(368,'840301 - Licences and software Expense'),
    'WAREHOUSE_LOST_MANUAL':(435,'630101 - Scrap'),
    'WAREHOUSE_LOST':(435,'630101 - Scrap'),
    'DisposalComplete':(435,'630101 - Scrap'),
    'RemovalComplete':(435,'630101 - Scrap'),
    'ShippingChargeback':(512,'610102 - Amazon Freight'),
    'ShippingHB':(512,'610102 - Amazon Freight'),
    'WarehousePrep':(517,'620103 - Packaging fees'),
    'PREPFEE_REFUND':(525,'640101 - Amazon Fees'),
    'RestockingFee':(525,'640101 - Amazon Fees'),
    'RefundCommission':(525,'640101 - Amazon Fees'),
    'Subscription Fee':(525,'640101 - Amazon Fees'),
    'Paid Services Fee':(525,'640101 - Amazon Fees'),
    'Bank Deposit':(1426,'181216 - RBC Georgia'),
    'Current Reserve Amount':(1535,'131003 - Deposits payment provider'),
    'Previous Reserve Amount Balance':(1535,'131003 - Deposits payment provider'),
    'FBAPerUnitFulfillmentFee':(512,'610102 - Amazon Freight'),
    'FBAInboundTransportationFee':(512,'610102 - Amazon Freight'),
    'Transfer of funds unsuccessful: An unexpected issue occurred. Please contact Seller Support for more information.':(1535,'131003 - Deposits payment provider'),
    'A2ZGuaranteeRecovery':(525,'640101 - Amazon Fees'),
    'StorageRenewalBilling':(350,'620101 - Storage'),
    'FBA transportation fee':(525,'640101 - Amazon Fees'),
    'Manual Processing Fee':(525,'640101 - Amazon Fees'),
    'TransactionTotalAmount':(336,'810309 - Advertising'),
    'MarketplaceFacilitatorTax-Shipping':(599,'840203 - Other Levies'),
    'MarketplaceFacilitatorTax-RestockingFee':(599,'840203 - Other Levies'),
    'MarketplaceFacilitatorTax-Other':(599,'840203 - Other Levies'),
    'MarketplaceFacilitatorVAT-Principal':(742,'137002 - Un-reconciled B2C Payments '),# added whitespace so that je2_amount can still be determined
    'MarketplaceFacilitatorVAT-Shipping':(742,'137002 - Un-reconciled B2C Payments '),
    'FBA Pick & Pack Fee':(525,'640101 - Amazon Fees')# added whitespace so that je2_amount can still be determined
    }#'MarketplaceFacilitatorTax-Principal':(599,'840203 - Other Levies'),

print('Summary of data tables:')
print('netsuite_product_data:',len(netsuite_product_data),'rows.')
print('netsuite_orders:',len(netsuite_orders),'rows.')
print('amazon_seller_center_report:',len(amazon_seller_center_report),'rows.')
print('amazon_statement_with_cust_name:',len(amazon_statement_with_cust_name),'rows.')
print('netsuite_payments:',len(netsuite_payments),'rows.')
print('netsuite_orders_tax:',len(netsuite_orders_tax),'rows.')
print('amount_description_types:',len(amount_description_types),'rows.')


if recon_country == 'US' :
    import Amazon_Order_Payment_Recon_US
else :
    import Amazon_Order_Payment_Recon_EMEA_CA

import Amazon_Brand_Fee_Recon

import Amazon_Marketplace_Recon_JE1

import Amazon_Brand_Fee_JE2
