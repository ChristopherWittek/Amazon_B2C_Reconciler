import pandas as pd
import os
import re
pd.options.display.float_format = '{:,.2f}'.format

print('>> B2C AMAZON RECONCILER APP <<')

input_recon_path = input('Copy & paste file-path to desired reconciliation folder:')
recon_path = input_recon_path
os.chdir(recon_path)
recon_path_data = recon_path+r'\Data'
path_parent = os.path.dirname(os.getcwd())

try :
    recon_period_market = re.findall('Amazon .*?$',recon_path)[0]
except :
    print('File path not correct format')

input_recon_country = input('Which country is being reconciled? (US, CA, DE, IT, ES, FR, UK, NL): ')

def region_recon(input_recon_country) :
    global recon_country
    recon_country = input_recon_country
    recon_regions = {'EMEA':'EMEA',
    'US':'Americas : NAM : United States',
    'CA':'Americas : NAM : Canada',
    'DE':'EMEA : Western Europe : Germany',
    'IT':'EMEA : Western Europe : Italy',
    'ES':'EMEA : Western Europe : Spain',
    'FR':'EMEA : Western Europe : France',
    'UK':'EMEA : Western Europe : United Kingdom',
    'NL':'EMEA : Western Europe : Netherlands'}
    for key,val in recon_regions.items() :
        if key == recon_country :
            global recon_region
            recon_region = val
    return recon_region
    return recon_country

region_recon(input_recon_country)

input_missing_orderids = input('Reconcile based on customer name due to missing order-ids in payment file? (y/n): ')

print('Reconciliation region:',recon_region)

import Amazon_Data_Cleansing
