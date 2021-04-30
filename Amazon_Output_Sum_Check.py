import pandas as pd
import os
pd.options.display.float_format = '{:,.2f}'.format

from Amazon_Brand_Fee_Recon import *
from Amazon_Marketplace_Recon_JE1 import *
from Amazon_Brand_Fee_JE2 import *
from Amazon_Data_Cleansing import *

print('>> JOURNAL ENTRY 1 & 2 SUM-CHECK vs AMAZON STATEMENT <<')

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

def ReconSumCheck(je1_amount,je2_amount,je1_sum_check,je2_sum_check,recon_amount_1,recon_amount_2) :
    cols = ['Journal','Amount ('+currency+')','Target('+currency+')','Difference ('+currency+')']
    sum_check_df = pd.DataFrame(columns=cols)
    je1_line = {'Journal':'Journal Entry 1','Amount ('+currency+')':je1_amount,'Target('+currency+')':recon_amount_1,'Difference ('+currency+')':je1_sum_check }
    je2_line = {'Journal':'Journal Entry 2','Amount ('+currency+')':je2_amount,'Target('+currency+')':recon_amount_2,'Difference ('+currency+')':je2_sum_check }
    sum_check_df = sum_check_df.append([je1_line,je2_line],ignore_index=True).set_index('Journal')
    print('> Journal Sum-Check <\n',sum_check_df)
    global recon_sum_check
    recon_sum_check = round(je1_amount + je2_amount - recon_target,2)
    cols = ['Journal Entry 1 ('+currency+')','Journal Entry 2 ('+currency+')','Amazon Statement ('+currency+')','Difference ('+currency+')']
    recon_total_check_df = pd.DataFrame(columns=cols)
    recon_total_check_line = {'Journal Entry 1 ('+currency+')':je1_amount,'Journal Entry 2 ('+currency+')':je2_amount,'Amazon Statement ('+currency+')':recon_target,'Difference ('+currency+')':recon_sum_check}
    recon_total_check_df = recon_total_check_df.append(recon_total_check_line,ignore_index=True).set_index('Journal Entry 1 ('+currency+')')
    print('> Recon Sum-Check <\n',recon_total_check_df)

ReconSumCheck(je1_amount,je2_amount,je1_sum_check,je2_sum_check,recon_amount_1,recon_amount_2)

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

    ReconSumCheck(je1_amount,je2_amount,je1_sum_check,je2_sum_check,recon_amount_1,recon_amount_2)

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
