import pandas as pd
import os
pd.options.display.float_format = '{:,.2f}'.format

from Amazon_Brand_Fee_Recon import *
from Amazon_Marketplace_Recon_JE1 import *
from Amazon_Brand_Fee_JE2 import *
from Amazon_Data_Cleansing import *

print('> JOURNAL ENTRY 1 & 2 SUM-CHECK vs AMAZON STATEMENT <')

##### JE1,JE2,JETotal #####
amount_type_fees_summary_amount_type = round(amount_type_fees_summary.groupby('amount-type').sum(),2)
recon_target = round(amount_type_fees_summary_amount_type.loc['Grand Total']['amount'],2)
try :
    recon_amount_1 = round(amount_type_fees_summary_amount_type.loc['ItemPrice']['amount'] + amount_type_fees_summary_amount_type.loc['Promotion']['amount'],2)
    recon_amount_2 = round(amount_type_fees_summary_amount_type.drop(['ItemPrice','Promotion','Grand Total']).sum()[0],2)
except Exception as e :
    recon_amount_1 = round(amount_type_fees_summary_amount_type.loc['ItemPrice']['amount'])
    recon_amount_2 = round(amount_type_fees_summary_amount_type.drop(['ItemPrice','Grand Total']).sum()[0],2)


def JournalBankSum(journal,*args) :
    bank_debit = journal.loc[journal['Account'] == '137002 - Un-reconciled B2C Payments','DR'].values[0]
    bank_credit = journal.loc[journal['Account'] == '137002 - Un-reconciled B2C Payments','CR'].values[0]
    bank_sum = round(bank_debit - bank_credit,2)
    return bank_sum

je1_amount = JournalBankSum(journal_entry1)
je2_amount = JournalBankSum(journal_entry2)
je1_sum_check = round(je1_amount - recon_amount_1,2)
je2_sum_check = round(je2_amount - recon_amount_2,2)

# journal_sum_check = round(je1_amount - recon_amount_1,2)

print('Journal Entry 1:',je1_amount,'|| Journal Entry 1 Target:',recon_amount_1,'|| Difference:',je1_sum_check )
print('Journal Entry 2:',je2_amount,'|| Journal Entry 2 Target:',recon_amount_2,'|| Difference:',je2_sum_check )
recon_sum_check = round(je1_amount + je2_amount - recon_target,2)
print('> Recon Sum-Check <')
print('Journal Entry 1:',je1_amount,'|| Journal Entry 2:',je2_amount,'|| Amazon Statement:',recon_target,'|| Sum-Check:',recon_sum_check)

if recon_sum_check != 0 :
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

        if je1_sum_check != 0 :
            journal_entry1 = journal_entry1.append(journal_line,ignore_index=True)
            journal_entry1.loc[journal_entry1['Account'] == '137002 - Un-reconciled B2C Payments','DR'] = 0
            unrec_payments_1 = round(abs(journal_entry1['CR'].sum() - journal_entry1['DR'].sum()),2)
            journal_entry1.loc[journal_entry1['Account'] == '137002 - Un-reconciled B2C Payments','DR'] = unrec_payments_1
        else :
            journal_entry2 = journal_entry2.append(journal_line,ignore_index=True)
            journal_entry2.loc[journal_entry2['Account'] == '137002 - Un-reconciled B2C Payments','CR'] = 0
            unrec_payments_2 = round(abs(journal_entry2['CR'].sum() - journal_entry2['DR'].sum()),2)
            journal_entry2.loc[journal_entry2['Account'] == '137002 - Un-reconciled B2C Payments','CR'] = unrec_payments_2

    je1_amount = JournalBankSum(journal_entry1)
    je2_amount = JournalBankSum(journal_entry2)
    je1_sum_check = round(je1_amount - recon_amount_1,2)
    je2_sum_check = round(je2_amount - recon_amount_2,2)

    print('journal_entry2:',len(journal_entry2),'rows.')
    recon_sum_check = round(je1_amount + je2_amount - recon_target,2)
    print('> Recon Sum-Check <')
    print('Journal Entry 1:',je1_amount,'|| Journal Entry 2:',je2_amount,'|| Amazon Statement:',recon_target,'|| Sum-Check:',recon_sum_check)

else :
    pass


journal_entry1['External ID'],journal_entry1['Department'],journal_entry1['Product'],journal_entry1['Brand'],journal_entry1['Technology'],journal_entry1['Channel'],journal_entry1['Region'] = recon_period_market,'12','Corporate','Corporate','Corporate','B2C : Mainstream Amazon',recon_region
journal_entry1 = journal_entry1.set_index('External ID').sort_values('Internal ID')
journal_entry2 = journal_entry2.set_index('External ID')#.sort_values('Internal ID')

journal_entry1.to_csv(recon_path+r'\Output Files\journal_entry1'+recon_country+'.csv')
journal_entry2.to_csv(recon_path+r'\Output Files\journal_entry2'+recon_country+'.csv')


if recon_country == 'US' :
    fee_comparison = amount_type_fees_summary.reset_index()
    fee_comparison = fee_comparison['amount-description']
    missing_fees = set(fee_comparison) - set(amount_description_types)
    print('Following fees could be missing for the US:',missing_fees)

# print('>> RECONCILIATION COMPLETED <<')
