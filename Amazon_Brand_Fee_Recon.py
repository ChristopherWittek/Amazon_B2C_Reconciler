import pandas as pd
from Amazon_Data_Cleansing import *
from Amazon_B2C_Reconciler_App import *# recon_path, recon_path_data, recon_period_market, recon_country, recon_region, path_parent
pd.options.mode.chained_assignment = None
pd.options.display.float_format = '{:,.2f}'.format
print('> PROCESSING AMAZON FEES & COMISSION <')

if recon_country == 'US' :
    from Amazon_Order_Payment_Recon_US import *
else :
    from Amazon_Order_Payment_Recon_EMEA_CA import *


amazon_statement_product_brand_cust_name = amazon_statement_with_cust_name.merge(netsuite_product_data,how='left',on='sku')
amazon_statement_product_brand_cust_name = amazon_statement_product_brand_cust_name[amazon_statement_product_brand_cust_name.index.duplicated(keep='first') == False]
try :
    amazon_statement_product_brand_cust_name = amazon_statement_product_brand_cust_name[(amazon_statement_product_brand_cust_name['amount'].str.contains('amount') == False)]
    amazon_statement_product_brand_cust_name['amount'] = amazon_statement_product_brand_cust_name['amount'].str.replace(',','').astype(float)
except :
    print('No strings in column "amount" in amazon_statement_product_brand_cust_name.')


amount_type_fees_summary = pd.pivot_table(amazon_statement_product_brand_cust_name,
index=['amount-type','transaction-type','amount-description'],
values='amount',
aggfunc='sum').fillna(0).astype(float)
amount_type_fees_summary.loc['Grand Total', :] = amount_type_fees_summary.sum().values


brand_commission_fulfillment_fees_summary = amazon_statement_product_brand_cust_name[
(amazon_statement_product_brand_cust_name['amount-description'] == 'Commission') |
 (amazon_statement_product_brand_cust_name['amount-description'] == 'FBAPerUnitFulfillmentFee')]
print('> SKUs missing from AMZ_Products:',brand_commission_fulfillment_fees_summary[(brand_commission_fulfillment_fees_summary['sku'].notna()) & (brand_commission_fulfillment_fees_summary['BRAND'].isna())]['sku'].unique(),'<')
brand_commission_fulfillment_fees_summary['BRAND'] = brand_commission_fulfillment_fees_summary['BRAND'].fillna('Multi-Brand')
brand_commission_fulfillment_fees_summary = pd.pivot_table(brand_commission_fulfillment_fees_summary,
index='BRAND',
columns='amount-description',
values='amount',
aggfunc='sum').fillna(0).astype(float)
brand_commission_fulfillment_fees_summary['Grand Total'] = brand_commission_fulfillment_fees_summary.sum(axis=1)
brand_commission_fulfillment_fees_summary.loc['Grand Total'] = brand_commission_fulfillment_fees_summary.sum()
brand_commission_fulfillment_fees_summary.to_csv('brand_commission_fulfillment_fees_summary.csv')


amazon_statement_product_brand_cust_name.to_csv('amazon_statement_product_brand_cust_name.csv')
clawback_reimbursement_breakdown = amazon_statement_product_brand_cust_name[
(amazon_statement_product_brand_cust_name['amount-description'] == 'COMPENSATED_CLAWBACK') |
(amazon_statement_product_brand_cust_name['amount-description'] == 'REVERSAL_REIMBURSEMENT')]
try :
    clawback_reimbursement_breakdown = clawback_reimbursement_breakdown[['Name','Internal ID_x','order-id','sku','amount','PRODUCT','BRAND','PRODUCT TYPE','amount-description']]
except :
    clawback_reimbursement_breakdown = clawback_reimbursement_breakdown[['Name','Internal ID','order-id','sku','amount','PRODUCT','BRAND','PRODUCT TYPE','amount-description']]
clawback_reimbursement_breakdown['PRODUCT'] = clawback_reimbursement_breakdown['PRODUCT'].fillna('Corporate')
clawback_reimbursement_breakdown['BRAND'] = clawback_reimbursement_breakdown['BRAND'].fillna('Multi-Brand')
clawback_reimbursement_breakdown['PRODUCT TYPE'] = clawback_reimbursement_breakdown['PRODUCT TYPE'].fillna('Corporate')
clawback_reimbursement_breakdown.to_csv('clawback_reimbursement_breakdown.csv')


print('brand_commission_fulfillment_fees_summary',brand_commission_fulfillment_fees_summary)
print('amount_type_fees_summary',amount_type_fees_summary)
print('clawback_reimbursement_breakdown',len(clawback_reimbursement_breakdown),'rows.')
