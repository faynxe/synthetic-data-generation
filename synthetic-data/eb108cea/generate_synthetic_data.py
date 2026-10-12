"""LC20 bounded-memory generator; CSV is the default output.
Dependencies: numpy, pandas, Faker, pyarrow, duckdb; boto3 for S3.
Configure using LC20_* environment variables. Default source configuration is
preserved. LC20_SELF_TEST=1 runs only 1000 rows/root and local end-to-end tests.
Global uniqueness is exact across batches and surviving incremental rows.
Finite-domain numeric bitmaps and string SQLite indexes are disk-backed;
parent keys are streamed from temporary Parquet, not held in RAM.
Incremental processing uses a memory-limited, spilling DuckDB database.
CSV S3 output streams multipart uploads. CSV local output is atomic per file.
No date windows, precisions, constraints, or cardinalities are relaxed.
Infeasible scales fail before generation; no 100M+ benchmark is attempted.
Random draws preserve the source distributions, not byte-identical source RNG
ordering. Enum/null percentage deviations remain warnings as in the source.
"""
import os, json, math, sqlite3, tempfile, shutil, warnings
from pathlib import Path
import numpy as np
import pandas as pd
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

SEED=int(os.getenv('LC20_SEED','42'))
SCALE_FACTOR=float(os.getenv('LC20_SCALE_FACTOR','1'))
S3_BUCKET=os.getenv('LC20_S3_BUCKET','mm-fsi-fix')
S3_PREFIX=os.getenv('LC20_S3_PREFIX','lc/synthetic_data_1/runs/20261011-223023/')
INCREMENTAL_PREFIX=os.getenv('LC20_INCREMENTAL_PREFIX',S3_PREFIX+'incremental/')
STORAGE=os.getenv('LC20_STORAGE','s3')
LOCAL_DIR=os.getenv('LC20_LOCAL_DIR','/tmp/synthetic_out')
OUTPUT_FORMAT=os.getenv('LC20_OUTPUT_FORMAT','csv')
MODE=os.getenv('LC20_MODE','full')
INCREMENTAL_OUTPUT=os.getenv('LC20_INCREMENTAL_OUTPUT','merged')
INSERT_PCT=float(os.getenv('LC20_INSERT_PCT','10'))
UPDATE_PCT=float(os.getenv('LC20_UPDATE_PCT','5'))
DELETE_PCT=float(os.getenv('LC20_DELETE_PCT','2'))
START_DATE=os.getenv('LC20_START_DATE','2025-01-01')
END_DATE=os.getenv('LC20_END_DATE','2025-11-28 23:59:59')
INCR_WINDOW_DAYS=int(os.getenv('LC20_INCR_WINDOW_DAYS','20'))
CHILD_LAG_DAYS,MOD_LAG_DAYS,EVENT_LAG_DAYS=7,60,30
FIRST_NAME_POOL,LAST_NAME_POOL=400,550
DOB_MIN,DOB_MAX='1934-12-07','2007-11-17'
MEMBER_REF_MIN,MEMBER_REF_MAX=10836,997603725
SSN_LO,SSN_HI=100_000_000,899_999_999
EMAIL_DOMAINS=['gmail.com','yahoo.com','outlook.com','hotmail.com','aol.com','icloud.com','example.com']
KEY_NAME_FEK='application-store_first_name_fek'
KEY_EMAIL_FEK='application-store_email_address_fek'
DIST_TOLERANCE,STRICT=.06,True
BATCH_ROWS=int(os.getenv('LC20_BATCH_ROWS','50000'))
WORK_DIR=os.getenv('LC20_WORK_DIR','/tmp/lc20_work')
DB_MEMORY=os.getenv('LC20_DB_MEMORY','1GB')
DB_THREADS=int(os.getenv('LC20_DB_THREADS','2'))
SELF_TEST=os.getenv('LC20_SELF_TEST','0')=='1'
DAY=86400
A='lc20_as.'
ROOT_ROW_COUNTS={A+'as_application':1000,'lc20_das.das_origination':1000,
 'lc20_ps.ps_prescreen_entry':1000,'lc20_uw_processing.uw_process':1000}
GOLDEN_TABLES={}
ROW_COUNT_OVERRIDES=json.loads(os.getenv('LC20_ROW_COUNT_OVERRIDES','{}'))
ORDER=[A+'as_application',A+'as_applicant',A+'as_applicant_detail',
 A+'as_application_certificate_of_d',A+'as_application_checking_account',
 A+'as_application_saving_account',A+'as_email_address',
 'lc20_das.das_origination','lc20_das.das_member_origination','lc20_das.das_origination_ext_ref',
 'lc20_ps.ps_prescreen_entry','lc20_ps.ps_prescreen_ext_ref',
 'lc20_uw_processing.uw_process','lc20_uw_processing.uw_process_run',
 'lc20_uw_processing.uw_process_run_ext_ref']
SPECS={
 A+'as_application':dict(cols='id:s created_d:t modified_d:t version:i application_d:t product_type_cd:s application_type_cd:s',
 idb=16,depth=0,fk=None,nonmut=['application_d'],unique=['id','created_d','modified_d','application_d'],ranges={'version':(0,1)},nulls={'product_type_cd':.904,'application_type_cd':.096},temporal=[]),
 A+'as_applicant':dict(cols='id:s created_d:t modified_d:t version:i application_id:s primary_applicant_flg:i sms_marketing_opt_in_flg:i marketing_attribution_cd:f',
 idb=16,depth=1,fk=('application_id',A+'as_application',1.,1,1),nonmut=[],unique=['id','created_d','modified_d','application_id'],ranges={'version':(0,1),'primary_applicant_flg':(1,1),'sms_marketing_opt_in_flg':(0,1)},nulls={'marketing_attribution_cd':1.},temporal=[]),
 A+'as_applicant_detail':dict(cols=('id:s created_d:t modified_d:t version:i applicant_id:s source_cd:s first_name:s first_name_keyn:s first_name_keyv:i '
 'last_name:s last_name_keyn:s last_name_keyv:i honorific_cd:f date_of_birth:d date_of_birth_keyn:s date_of_birth_keyv:i ssn:s ssn_keyn:s ssn_keyv:i '
 'marital_status_cd:f educational_qualification_cd:f employment_status_cd:s cumulative_job_tenure_months:i annual_income:f annual_additional_income:f '
 'annual_income_taxable:f annual_income_non_taxable:f housing_status_cd:s annual_housing_expense:f annual_additional_debt:f fico_range_cd:f fico_min:i fico_max:i citizenship:f'),
 idb=16,depth=2,fk=('applicant_id',A+'as_applicant',1.,1,1),nonmut=['ssn'],unique=['id','created_d','modified_d','applicant_id','ssn','annual_income','annual_additional_income','annual_income_taxable','annual_income_non_taxable','annual_housing_expense','annual_additional_debt'],
 ranges={'version':(0,10),'first_name_keyv':(1,1),'last_name_keyv':(1,1),'date_of_birth_keyv':(1,1),'ssn_keyv':(1,1),'cumulative_job_tenure_months':(1,20),'annual_income':(13.5021,9998.64691),'annual_additional_income':(6.20378,9947.23003),'annual_income_taxable':(1.21818,9997.39902),'annual_income_non_taxable':(7.48881,9994.96678),'annual_housing_expense':(23.61788,9996.5062),'annual_additional_debt':(.55862,9982.90525),'fico_min':(1,5),'fico_max':(1,5)},
 nulls={'honorific_cd':1.,'marital_status_cd':1.,'educational_qualification_cd':1.,'employment_status_cd':.102,'housing_status_cd':.106,'fico_range_cd':1.,'citizenship':1.},temporal=[]),
 A+'as_application_certificate_of_d':dict(cols='id:s created_d:t modified_d:t version:i source_cd:s initial_deposit_amount:i product_code:i application_id:s apy:f term_in_months:i is_joint_flg:i account_type:s',
 idb=9,depth=1,fk=('application_id',A+'as_application',1.,1,1),nonmut=[],unique=['id','created_d','modified_d','application_id'],ranges={'version':(0,5),'initial_deposit_amount':(155,99836),'product_code':(6,28),'apy':(3.4,5.),'term_in_months':(6,60),'is_joint_flg':(0,1)},nulls={},temporal=[]),
 A+'as_application_checking_account':dict(cols='id:s created_d:t modified_d:t version:i application_id:s source_cd:s debit_card_requested_flg:i min_initial_deposit_amount:i initial_deposit_amount:i product_code:i is_joint_flg:i account_type:s',
 idb=9,depth=1,fk=('application_id',A+'as_application',1.,1,1),nonmut=[],unique=['id','created_d','modified_d','application_id'],ranges={'version':(0,10),'debit_card_requested_flg':(0,1),'min_initial_deposit_amount':(1,999),'initial_deposit_amount':(3,998),'product_code':(1,53),'is_joint_flg':(0,1)},nulls={},temporal=[]),
 A+'as_application_saving_account':dict(cols='id:s created_d:t modified_d:t version:i source_cd:s application_id:s initial_deposit_amount:i product_code:i is_joint_flg:i debit_card_requested_flg:i account_type:s',
 idb=9,depth=1,fk=('application_id',A+'as_application',1.,1,1),nonmut=[],unique=['id','created_d','modified_d','application_id'],ranges={'version':(1,10),'initial_deposit_amount':(19,9989),'product_code':(9,25),'is_joint_flg':(0,1),'debit_card_requested_flg':(0,1)},nulls={},temporal=[]),
 A+'as_email_address':dict(cols='id:s created_d:t modified_d:t version:i applicant_id:s source_cd:s email_address:s email_address_keyn:s email_address_keyv:i',
 idb=16,depth=2,fk=('applicant_id',A+'as_applicant',1.,1,1),nonmut=['email_address'],unique=['id','created_d','modified_d','applicant_id','email_address'],ranges={'version':(0,20),'email_address_keyv':(1,1)},nulls={},temporal=[]),
 'lc20_das.das_origination':dict(cols='id:s created_d:t modified_d:t version:i origination_guid:s',idb=16,depth=0,fk=None,nonmut=['origination_guid'],unique=['id','created_d','modified_d','origination_guid'],ranges={'version':(0,0)},nulls={},temporal=[]),
 'lc20_das.das_member_origination':dict(cols='id:s created_d:t modified_d:t version:i origination_id:s member_ref_id:i member_src_id:i member_entity_name:s',
 idb=16,depth=1,fk=('origination_id','lc20_das.das_origination',1.,1,1),nonmut=['member_ref_id'],unique=['id','modified_d','origination_id','member_ref_id'],ranges={'version':(0,0),'member_ref_id':(MEMBER_REF_MIN,MEMBER_REF_MAX),'member_src_id':(1000,1200)},nulls={},temporal=[]),
 'lc20_das.das_origination_ext_ref':dict(cols='id:s created_d:t modified_d:t version:i origination_id:s ext_ref_id:s ext_src_id:i ext_entity_name:s flow_id:s flow_version:f',
 idb=16,depth=1,fk=('origination_id','lc20_das.das_origination',2.,2,2),nonmut=['ext_ref_id','ext_src_id','ext_entity_name'],unique=['id','created_d','modified_d','ext_ref_id'],ranges={'version':(0,5),'ext_src_id':(11000,11004),'flow_version':(0,5)},nulls={'flow_id':.108,'flow_version':.519},temporal=[]),
 'lc20_ps.ps_prescreen_entry':dict(cols='id:s created_d:t modified_d:t version:i source_cd:s credit_policy_name:s credit_policy_version:i effective_start_d:t effective_end_d:f status_cd:f first_look_up_d:t last_look_up_d:t used_d:t offer_amt:f offer_purpose_cd:s',
 idb=16,depth=0,fk=None,nonmut=[],unique=['id','created_d','modified_d','effective_start_d','first_look_up_d','last_look_up_d','used_d','offer_amt'],ranges={'version':(1,862),'credit_policy_version':(100,344),'offer_amt':(70.05,99876.82)},nulls={'source_cd':.42,'credit_policy_name':.581,'effective_end_d':1.,'status_cd':1.,'offer_purpose_cd':.406},temporal=[('first_look_up_d','last_look_up_d'),('last_look_up_d','used_d')]),
 'lc20_ps.ps_prescreen_ext_ref':dict(cols='id:s created_d:t modified_d:t version:i ps_prescreen_entry_id:s ext_src_id:s ext_ref_id:s ext_entity_name:s ext_ref_direction_cd:s',
 idb=16,depth=1,fk=('ps_prescreen_entry_id','lc20_ps.ps_prescreen_entry',1.,1,1),nonmut=['ext_ref_id'],unique=['id','created_d','modified_d','ps_prescreen_entry_id','ext_ref_id'],ranges={'version':(0,0)},nulls={'ext_ref_direction_cd':.101},temporal=[]),
 'lc20_uw_processing.uw_process':dict(cols='id:s created_d:t modified_d:t version:i subject_ref_id:s subject_entity_name:s type_cd:s subject_src_id:s status_cd:s product_type_cd:f status_d:t decision_status_cd:s decision_status_d:t decision_phase_cd:s subject_type_cd:s expiration_d:t',
 idb=16,depth=0,fk=None,nonmut=['subject_ref_id','subject_src_id'],unique=['id','created_d','modified_d','subject_ref_id','subject_src_id','status_d','decision_status_d','expiration_d'],ranges={'version':(1,10)},nulls={'type_cd':.403,'product_type_cd':1.,'decision_phase_cd':.097},temporal=[('status_d','decision_status_d'),('decision_status_d','expiration_d')]),
 'lc20_uw_processing.uw_process_run':dict(cols='id:s created_d:t modified_d:t version:i uw_process_id:s decision_phase_cd:s decision_status_cd:s',idb=16,depth=1,fk=('uw_process_id','lc20_uw_processing.uw_process',1.,1,1),nonmut=[],unique=['id','created_d','modified_d','uw_process_id'],ranges={'version':(1,10)},nulls={},temporal=[]),
 'lc20_uw_processing.uw_process_run_ext_ref':dict(cols='id:s created_d:t modified_d:t version:i uw_process_run_id:s ext_ref_id:s ext_src_id:s ext_ref_entity_name:s',idb=16,depth=2,fk=('uw_process_run_id','lc20_uw_processing.uw_process_run',1.,1,1),nonmut=['ext_ref_id','ext_src_id'],unique=['id','created_d','modified_d','uw_process_run_id','ext_ref_id','ext_src_id'],ranges={'version':(1,10)},nulls={},temporal=[])
}
COLS={t:[tuple(x.split(':')) for x in s['cols'].split()] for t,s in SPECS.items()}
ENUMS={
 A+'as_application':{'version':{0:.532,1:.468},'application_type_cd':{'JOINT_CERTIFICATE_DEPOSIT_ACCOUNT':.281,'JOINT_SAVINGS_ACCOUNT':.2456,'JOINT_CHECKING_ACCOUNT':.2179,'CERTIFICATE_DEPOSIT_ACCOUNT':.1582,'SAVINGS_ACCOUNT':.0531,'CHECKING_ACCOUNT':.0442}},
 A+'as_applicant':{'version':{0:.527,1:.473},'sms_marketing_opt_in_flg':{0:.506,1:.494}},
 A+'as_applicant_detail':{'version':{0:.104,5:.098,6:.097,4:.096,1:.095,3:.094,10:.09,9:.088,8:.081,2:.079,7:.078},'employment_status_cd':{'OTHER':.1826,'RETIRED':.1804,'UNEMPLOYED':.1771,'EMPLOYED':.1648,'SELFEMPLOYED':.1537,'DISABLED':.1414},'housing_status_cd':{'OTHER':.1801,'OWN':.1723,'NONE':.1689,'RENT':.1633,'ANY':.1588,'MORTGAGE':.1566},'fico_min':{3:.221,5:.211,2:.201,1:.192,4:.175},'fico_max':{5:.231,3:.219,2:.188,1:.181,4:.181}},
 A+'as_application_certificate_of_d':{'version':{5:.185,4:.173,3:.171,1:.167,0:.162,2:.142},'product_code':{25:.134,21:.12,14:.115,28:.115,6:.113,22:.112,12:.103,23:.099,26:.089},'is_joint_flg':{0:.528,1:.472}},
 A+'as_application_checking_account':{'version':{1:.102,2:.101,9:.095,4:.094,0:.093,3:.09,8:.089,6:.089,10:.087,5:.086,7:.074},'debit_card_requested_flg':{0:.527,1:.473},'product_code':{1:.506,53:.494},'is_joint_flg':{0:.506,1:.494}},
 A+'as_application_saving_account':{'version':{1:.115,3:.106,8:.102,10:.101,6:.101,2:.097,7:.097,5:.095,9:.094,4:.092},'product_code':{25:.362,10:.322,9:.316},'is_joint_flg':{0:.527,1:.473},'debit_card_requested_flg':{0:.506,1:.494}},
 A+'as_email_address':{'version':{20:.063,12:.058,14:.057,3:.056,4:.053,15:.053,6:.053,5:.052,17:.049,7:.048,18:.048,9:.048,1:.046,10:.044,19:.043,8:.043,11:.041,2:.039,16:.036,13:.035,0:.035}},
 'lc20_das.das_member_origination':{'member_src_id':{1200:.508,1000:.492},'member_entity_name':{'pfloginuserid':.505,'loginactorid':.495}},
 'lc20_das.das_origination_ext_ref':{'version':{0:.179,1:.178,5:.167,4:.166,3:.16,2:.15},'flow_id':{'jointCertificateDepositAccountFlow':.1446,'checkingAccountFlow':.1334,'savingsAccountFlow':.1312,'jointCheckingAccountFlow':.1256,'jointSavingsAccountFlow':.1211,'creditInABoxFlow':.1211,'certificateDepositAccountFlow':.1177,'balanceTransferFlow':.1054},'flow_version':{0.:.4096,5.:.1809,4.:.1143,1.:.106,3.:.1019,2.:.0873}},
 'lc20_ps.ps_prescreen_entry':{'source_cd':{'LIGHTBOX_AUTO':.0879,'EASYAPPLY':.0845,'PL_PRE_APPROVAL':.0793,'PL2AUTO':.0776,'PROSPECT':.0776,'PL2BT':.0759,'PL2PL_MB_TOPUP':.0724,'CLEANSWEEP':.069,'PL2PL_CP_TOPUP':.0672,'JOINT_SAVINGS_ACCOUNT':.0655,'PL2PL_TOPUP':.0638,'PLDECLINE2AUTO':.0621,'EXPERIAN_CREDIT_MATCH':.0586,'LIGHTBOX':.0586},'credit_policy_name':{'cleansweep':.3365,'PL':.3365,'personal':.327},'offer_purpose_cd':{'home_improvement':.0926,'credit_card':.0791,'wedding':.0774,'renewable_energy':.0774,'educational':.0741,'debt_consolidation':.0707,'small_business':.0673,'major_purchase':.0673,'not_requested':.0673,'other':.0606,'moving':.0572,'house':.0572,'vacation':.0556,'car':.0522,'medical':.0438}},
 'lc20_ps.ps_prescreen_ext_ref':{'ext_src_id':{'11004':.817,'11001':.053,'11002':.03,'11008':.027,'14000':.026,'1000':.024,'PL2BT':.023},'ext_entity_name':{'applicationid':.3,'actor':.241,'loan':.196,'referrer':.106,'bpsrequestid':.097,'program':.012,'bankaccount':.011,'accounttype':.008,'product':.007,'loanapp':.006,'loginactorid':.006,'cif':.006,'productcode':.004},'ext_ref_direction_cd':{'SOURCE':.8354,'TARGET':.1646}},
 'lc20_uw_processing.uw_process':{'version':{2:.114,5:.112,9:.111,8:.108,10:.099,4:.095,6:.095,3:.091,7:.089,1:.086},'subject_entity_name':{'actor':.359,'applicationid':.326,'address':.315},'type_cd':{'APPLICATION_DECISION':.526,'ADDRESS_VALIDATION':.474},'status_cd':{'SUCCESS':.413,'NEW':.111,'ACTIVE':.104,'FAILURE':.102,'EXPIRED':.1,'WITHDRAWN':.092,'CANCELED':.078},'decision_status_cd':{'SUCCESS':.443,'PENDING':.168,'ERROR':.102,'MORE_INFO':.101,'FAILURE':.096,'IN_PROGRESS':.09},'decision_phase_cd':{'DRAW_VERIFICATION':.1406,'FULL_DECISION':.1362,'FULFILLMENT':.1285,'ADDRESS_VALIDATION':.1285,'UNKNOWN':.1218,'BE':.1218,'FE':.1196,'BANK_ACCOUNT':.103},'subject_type_cd':{'JOINT_CERTIFICATE_DEPOSIT_ACCOUNT':.236,'CERTIFICATE_DEPOSIT_ACCOUNT':.16,'JOINT_CHECKING_ACCOUNT':.156,'SAVINGS_ACCOUNT':.111,'JOINT_SAVINGS_ACCOUNT':.093,'CHECKING_ACCOUNT':.071,'ADDRESS_BUSINESS':.064,'ADDRESS':.055,'CS_ORDER':.054}},
 'lc20_uw_processing.uw_process_run':{'version':{2:.112,1:.109,9:.109,4:.103,5:.102,6:.097,7:.097,3:.095,8:.089,10:.087},'decision_phase_cd':{'ADDRESS_VALIDATION':.257,'FULFILLMENT':.254,'FULL_DECISION':.247,'BANK_ACCOUNT':.242},'decision_status_cd':{'PENDING':.191,'MORE_INFO':.169,'SUCCESS':.168,'FAILURE':.167,'IN_PROGRESS':.159,'ERROR':.146}},
 'lc20_uw_processing.uw_process_run_ext_ref':{'version':{1:.129,4:.11,7:.107,3:.106,9:.101,8:.1,2:.094,10:.092,5:.081,6:.08},'ext_ref_entity_name':{'journeyapplicationtoken':.122,'loginactorid':.119,'narmiuuid':.116,'offerid':.114,'entitytoken':.113,'bankaccount':.112,'applicantid':.103,'prescreenentry':.102,'cif':.099}}
}
POOLS={}

def ts(s): return int(pd.Timestamp(s).value//10**9)

def init_pools(seed):
    fake=Faker('en_US'); fake.seed_instance(seed)
    for key,fn,size,lo,hi in [('first',fake.first_name,FIRST_NAME_POOL,3,11),('last',fake.last_name,LAST_NAME_POOL,2,11)]:
        out={}
        for _ in range(50000):
            nm=fn()
            if lo<=len(nm)<=hi and nm.isalpha(): out[nm]=None
            if len(out)==size: break
        if len(out)!=size: raise RuntimeError('Name pool could not be populated')
        POOLS[key]=np.array(list(out),dtype=object)

def counts_full():
    counts={}
    for t in ORDER:
        if t in GOLDEN_TABLES: counts[t]=int(GOLDEN_TABLES[t])
        elif t in ROW_COUNT_OVERRIDES: counts[t]=int(ROW_COUNT_OVERRIDES[t])
        elif t in ROOT_ROW_COUNTS: counts[t]=int(round(ROOT_ROW_COUNTS[t]*SCALE_FACTOR))
        else: counts[t]=int(round(counts[SPECS[t]['fk'][1]]*SPECS[t]['fk'][2]))
    return counts

def preflight(counts,win,insert_counts=None):
    problems=[]
    for t,n in counts.items():
        if n<0: problems.append(t+': negative count'); continue
        sp=SPECS[t]
        if sp['fk']:
            _,p,avg,mn,mx=sp['fk']
            if mn!=mx or n!=mn*counts[p]: problems.append(t+': count override conflicts with exact cardinality')
        capacity=win['end']-(4-sp['depth'])*DAY-win['start']+1
        if insert_counts is None:
            if n>capacity: problems.append(f'{t}: {n:,} rows exceed creation timestamp capacity {capacity:,}')
        elif insert_counts[t]>capacity: problems.append(f'{t}: insert window holds at most {capacity:,} unique created_d values')
        for c in sp['unique']:
            if c=='ssn': cap=SSN_HI-SSN_LO
            elif c=='member_ref_id': cap=MEMBER_REF_MAX-MEMBER_REF_MIN+1
            elif dict(COLS[t])[c]=='f':
                dec=2 if c=='offer_amt' else 5; lo,hi=sp['ranges'][c]
                cap=int(round(hi*10**dec))-int(round(lo*10**dec))+1
            else: continue
            if n>cap: problems.append(f'{t}.{c}: {n:,} rows exceed {cap:,} distinct allowed values')
    if problems: raise ValueError('Infeasible configuration; rules were NOT relaxed:\n'+'\n'.join(problems))

def coerce(df,t):
    if list(df.columns)!=[c for c,k in COLS[t]]: raise ValueError(f'{t}: schema/column order mismatch')
    for c,k in COLS[t]:
        if k=='i': df[c]=pd.to_numeric(df[c],errors='raise').astype('int64')
        elif k=='f': df[c]=pd.to_numeric(df[c],errors='raise').astype('float64')
        elif k in ('t','d'):
            if pd.api.types.is_numeric_dtype(df[c]): df[c]=pd.to_datetime(df[c],unit='s')
            else: df[c]=pd.to_datetime(df[c],format='%Y-%m-%d %H:%M:%S' if k=='t' else '%Y-%m-%d',errors='raise')
            df[c]=df[c].astype('datetime64[ns]')
        else: df[c]=df[c].astype(object).where(df[c].notna(),None)
    return df

def output_frame(df,t):
    df=df.copy()
    for c,k in COLS[t]:
        if k=='t': df[c]=df[c].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif k=='d': df[c]=df[c].dt.strftime('%Y-%m-%d')
    return df

def validate_batch(df,t,win=None,parent_secs=None,is_update=False):
    sp=SPECS[t]
    for c,k in COLS[t]:
        if c not in sp['nulls'] and df[c].isna().any(): raise AssertionError(f'{t}.{c}: required')
    for c,(lo,hi) in sp['ranges'].items():
        if ((df[c]<lo)|(df[c]>hi)).any(): raise AssertionError(f'{t}.{c}: range')
    for b,a in [('created_d','modified_d')]+sp['temporal']:
        if (df[a]<df[b]).any(): raise AssertionError(f'{t}: temporal {b} <= {a}')
    for c,freq in ENUMS.get(t,{}).items():
        if not df[c].dropna().isin(freq).all(): raise AssertionError(f'{t}.{c}: enum')
    if win is not None:
        for c,k in COLS[t]:
            if k=='t' and not(is_update and c in {'created_d'}|set(sp['nonmut'])):
                vals=df[c].astype('int64')//10**9
                if ((vals<win['start'])|(vals>win['end'])).any(): raise AssertionError(f'{t}.{c}: window')
        if not is_update and ((df.modified_d-df.created_d).dt.total_seconds()>MOD_LAG_DAYS*DAY).any(): raise AssertionError('Modification lag')
    if parent_secs is not None:
        lag=df.created_d.astype('int64').to_numpy()//10**9-parent_secs
        if ((lag<0)|(lag>CHILD_LAG_DAYS*DAY)).any(): raise AssertionError('Child lag')
    if t==A+'as_applicant_detail':
        if not df.ssn.str.fullmatch(r'\d{3}-\d{2}-\d{4}').all(): raise AssertionError('SSN format')
        for c,lo,hi in [('first_name',3,11),('last_name',2,11)]:
            if not(df[c].str.len().between(lo,hi)&df[c].str.isalpha()).all(): raise AssertionError('Name format')
        if not df.date_of_birth.between(pd.Timestamp(DOB_MIN),pd.Timestamp(DOB_MAX)).all(): raise AssertionError('DOB range')
    if t==A+'as_email_address' and not df.email_address.str.len().between(16,32).all(): raise AssertionError('Email length')
    if not df.id.str.fullmatch('[0-9a-f]{'+str(sp['idb']*2)+'}').all(): raise AssertionError('ID format')

class Statistics:
    def __init__(self,t):
        self.t=t; self.n=0; self.nulls={c:0 for c in SPECS[t]['nulls']}; self.freq={c:{} for c in ENUMS.get(t,{})}
    def add(self,df):
        self.n+=len(df)
        for c in self.nulls: self.nulls[c]+=int(df[c].isna().sum())
        for c in self.freq:
            for v,n in df[c].dropna().value_counts().items(): self.freq[c][v]=self.freq[c].get(v,0)+int(n)
    def finish(self):
        warns=[]
        if self.n>=500:
            for c,p in SPECS[self.t]['nulls'].items():
                if abs(self.nulls[c]/self.n-p)>.05: warns.append(f'{self.t}.{c}: null probability deviation')
            for c,freq in ENUMS.get(self.t,{}).items():
                denom=sum(self.freq[c].values()) or 1; total=sum(freq.values())
                dev=max(abs(self.freq[c].get(v,0)/denom-p/total) for v,p in freq.items())
                if dev>DIST_TOLERANCE: warns.append(f'{self.t}.{c}: enum deviation {dev:.4f}')
        return {'rows':self.n,'warnings':warns,'null_counts':self.nulls,
                'enum_counts':{c:{str(v):n for v,n in vs.items()} for c,vs in self.freq.items()}}

class Bitmap:
    """Exact finite-domain membership; one disk-backed bit per possible value."""
    def __init__(self,path,lo,hi):
        self.lo=int(lo); self.hi=int(hi); self.count=0
        if self.hi<self.lo: raise ValueError('Empty uniqueness domain')
        length=(self.hi-self.lo+8)//8
        with open(path,'wb') as f: f.truncate(length)
        self.bits=np.memmap(path,dtype='uint8',mode='r+',shape=(length,))
    def reserve(self,values):
        values=np.asarray(values,dtype=np.int64); off=values-self.lo
        if ((off<0)|(values>self.hi)).any(): raise ValueError('Unique value outside original finite domain')
        _,first=np.unique(off,return_index=True)
        cand=off[first]; byte=cand>>3; mask=(1<<(cand&7)).astype(np.uint8)
        free=(self.bits[byte]&mask)==0; chosen=first[free]
        np.bitwise_or.at(self.bits,byte[free],mask[free])
        accepted=np.zeros(len(values),dtype=bool); accepted[chosen]=True; self.count+=len(chosen)
        return accepted
    def close(self): self.bits.flush(); self.bits._mmap.close()

class Uniqueness:
    """Per-table global exact indexes, released when each table finishes."""
    def __init__(self,t,work,time_lo,time_hi):
        self.t=t; self.path=Path(tempfile.mkdtemp(prefix='unique-',dir=work)); self.maps={}; self.domains={}; self.scale={}
        sp=SPECS[t]; fkc=sp['fk'][0] if sp['fk'] else None; self.cols=[c for c in sp['unique'] if c!=fkc]
        if 'created_d' not in self.cols: self.cols.append('created_d')
        for c in self.cols:
            k=dict(COLS[t])[c]
            if k=='t': bounds=(time_lo,time_hi); factor=1
            elif c=='ssn': bounds=(SSN_LO,SSN_HI-1); factor=1
            elif c=='member_ref_id': bounds=(MEMBER_REF_MIN,MEMBER_REF_MAX); factor=1
            elif k=='f':
                factor=10**(2 if c=='offer_amt' else 5); lo,hi=sp['ranges'][c]; bounds=(int(round(lo*factor)),int(round(hi*factor)))
            else: continue
            self.domains[c]=bounds; self.scale[c]=factor
        self.strings=sqlite3.connect(self.path/'strings.sqlite')
        for setting in ['journal_mode=OFF','synchronous=OFF','cache_size=-16384','temp_store=FILE']: self.strings.execute('PRAGMA '+setting)
        for c in self.cols:
            if c not in self.domains: self.strings.execute(f'CREATE TABLE "{c}" (v TEXT PRIMARY KEY) WITHOUT ROWID')
    def bitmap(self,c):
        if c not in self.maps: self.maps[c]=Bitmap(self.path/(c+'.bits'),*self.domains[c])
        return self.maps[c]
    def reserve(self,c,vals):
        if c in self.domains: return self.bitmap(c).reserve(vals)
        vals=np.asarray(vals,dtype=object); good=np.zeros(len(vals),dtype=bool)
        for start in range(0,len(vals),400):
            part=vals[start:start+400]
            sql=f'INSERT OR IGNORE INTO "{c}" (v) VALUES '+','.join(['(?)']*len(part))+' RETURNING v'
            accepted={v for (v,) in self.strings.execute(sql,[str(v) for v in part]).fetchall()}; seen=set()
            for i,v in enumerate(part):
                if v in accepted and v not in seen: good[start+i]=True; seen.add(v)
        return good
    def seed(self,df):
        for c in self.cols:
            vals=df[c]
            if c=='ssn': vals=vals.str.replace('-','',regex=False).astype('int64').to_numpy()
            elif dict(COLS[self.t])[c]=='t': vals=vals.astype('int64').to_numpy()//10**9
            elif dict(COLS[self.t])[c]=='f': vals=np.rint(vals.to_numpy()*self.scale[c]).astype('int64')
            else: vals=vals.to_numpy()
            if not self.reserve(c,vals).all(): raise AssertionError(f'{self.t}.{c}: existing duplicates')
    def draw(self,c,n,fn):
        if c in self.domains:
            bm=self.bitmap(c)
            if n>bm.hi-bm.lo+1-bm.count: raise ValueError(f'{self.t}.{c}: uniqueness domain exhausted')
        result=np.empty(n,dtype=np.int64 if c in self.domains else object); pending=np.arange(n)
        for _ in range(2000):
            if not len(pending): return result
            vals=fn(pending); good=self.reserve(c,vals); result[pending[good]]=vals[good]; pending=pending[~good]
        raise RuntimeError(f'{self.t}.{c}: no unique assignment after 2000 retries; constraints not relaxed')
    def times(self,c,rng,lo,hi):
        lo=np.asarray(lo,dtype=np.int64); hi=np.asarray(hi,dtype=np.int64)
        if (hi<lo).any(): raise ValueError(f'{self.t}.{c}: empty allowed timestamp interval')
        return self.draw(c,len(lo),lambda p:lo[p]+(rng.random(len(p))*(hi[p]-lo[p]+1)).astype(np.int64))
    def money(self,c,n,rng):
        lo,hi=SPECS[self.t]['ranges'][c]; fac=self.scale[c]
        return self.draw(c,n,lambda p:np.rint(np.round(rng.uniform(lo,hi,len(p)),int(round(math.log10(fac))))*fac).astype(np.int64))/fac
    def hex(self,c,n,rng,nbytes=16):
        def fn(p):
            raw=rng.integers(0,256,size=(len(p),nbytes),dtype=np.uint8)
            return np.frombuffer(raw.tobytes().hex().encode('ascii'),dtype=f'S{2*nbytes}').astype(str).astype(object)
        return self.draw(c,n,fn)
    def close(self):
        for bm in self.maps.values(): bm.close()
        self.strings.close(); shutil.rmtree(self.path)

def generate(t,n,rng,win,unique,fk=None,parent_secs=None,rank=None,old=None):
    sp=SPECS[t]; R=sp['ranges']; E=ENUMS.get(t,{})
    fkc=sp['fk'][0] if sp['fk'] else None; updating=old is not None
    skip=({'id','created_d','version',fkc}|set(sp['nonmut'])) if updating else set()
    d={c:old[c].to_numpy(copy=True) for c in skip if c and c in old} if updating else {}
    hi_c=win['end']-(4-sp['depth'])*DAY
    if not updating:
        lo=np.full(n,win['start'],dtype=np.int64) if parent_secs is None else np.maximum(parent_secs,win['start'])
        hi=np.full(n,hi_c,dtype=np.int64) if parent_secs is None else np.minimum(lo+CHILD_LAG_DAYS*DAY,hi_c)
        base=unique.times('created_d',rng,lo,hi); d['created_d']=base; d['id']=unique.hex('id',n,rng,sp['idb'])
        d['modified_d']=unique.times('modified_d',rng,base+1,np.minimum(base+MOD_LAG_DAYS*DAY,win['end']))
    else:
        base=rng.integers(win['start'],hi_c+1,n)
        d['modified_d']=unique.times('modified_d',rng,base+1,np.minimum(base+MOD_LAG_DAYS*DAY,win['end']))
        d['version']=np.minimum(old.version.to_numpy()+1,R['version'][1])
    if fkc and not updating: d[fkc]=fk
    def const(v): return np.full(n,v,dtype=object)
    def pick(freq):
        vals=np.array(list(freq)); p=np.array(list(freq.values()),dtype=float); p/=p.sum()
        return vals[rng.choice(len(vals),n,p=p)]
    for c,k in COLS[t]:
        if c in d: continue
        if c in E: d[c]=pick(E[c])
        elif c in sp['nulls'] and sp['nulls'][c]==1.: d[c]=np.full(n,np.nan)
        elif c in R:
            lo,hi=R[c]
            if c in unique.domains and k=='f': d[c]=unique.money(c,n,rng)
            elif c=='member_ref_id': d[c]=unique.draw(c,n,lambda p:np.exp(rng.uniform(np.log(MEMBER_REF_MIN),np.log(MEMBER_REF_MAX),len(p))).astype(np.int64))
            elif k=='i': d[c]=rng.integers(lo,hi+1,n)
            elif c=='apy': d[c]=rng.integers(int(lo*100),int(hi*100)+1,n)/100.
    def setval(c,v):
        if c not in skip: d[c]=v
    def event(c,b,offset,lag):
        if c not in skip: d[c]=unique.times(c,rng,b+1,np.minimum(b+lag*DAY,win['end']-offset*DAY))
    if t==A+'as_application':
        event('application_d',base,3,1); setval('product_type_cd',const('PERSONAL_LOAN'))
    elif t==A+'as_applicant': pass
    elif t==A+'as_applicant_detail':
        setval('source_cd',const('APPLICANT')); setval('first_name',rng.choice(POOLS['first'],n)); setval('last_name',rng.choice(POOLS['last'],n))
        for c in ['first_name_keyn','last_name_keyn','date_of_birth_keyn','ssn_keyn']: setval(c,const(KEY_NAME_FEK))
        setval('date_of_birth',rng.integers(ts(DOB_MIN)//DAY,ts(DOB_MAX)//DAY+1,n)*DAY)
        if 'ssn' not in skip:
            v=unique.draw('ssn',n,lambda p:rng.integers(SSN_LO,SSN_HI,len(p)))
            d['ssn']=np.char.add(np.char.add(np.char.add(np.char.add(np.char.zfill((v//1000000).astype(str),3),'-'),np.char.zfill(((v//10000)%100).astype(str),2)),'-'),np.char.zfill((v%10000).astype(str),4)).astype(object)
    elif t in [A+'as_application_certificate_of_d',A+'as_application_checking_account',A+'as_application_saving_account']:
        setval('source_cd',const('APPLICANT')); setval('account_type',const({A+'as_application_certificate_of_d':'CD',A+'as_application_checking_account':'ND',A+'as_application_saving_account':'SV'}[t]))
    elif t==A+'as_email_address':
        setval('source_cd',const('APPLICANT')); setval('email_address_keyn',const(KEY_EMAIL_FEK))
        if 'email_address' not in skip:
            def emails(p):
                k=len(p); names=np.char.lower(rng.choice(POOLS['first'],k).astype(str)); nums=rng.integers(0,10**6,k).astype(str)
                domains=rng.choice(np.array(EMAIL_DOMAINS),k); locals_=np.char.add(names,nums)
                for _ in range(10):
                    short=np.char.str_len(locals_)+1+np.char.str_len(domains)<16
                    if not short.any(): break
                    locals_=locals_.astype('U24'); locals_[short]=np.char.add(locals_[short],rng.integers(0,10,int(short.sum())).astype(str))
                return np.char.add(np.char.add(locals_,'@'),domains).astype(object)
            d['email_address']=unique.draw('email_address',n,emails)
    elif t=='lc20_das.das_origination':
        if 'origination_guid' not in skip: d['origination_guid']=unique.hex('origination_guid',n,rng)
    elif t=='lc20_das.das_member_origination': pass
    elif t=='lc20_das.das_origination_ext_ref':
        if 'ext_ref_id' not in skip: d['ext_ref_id']=unique.hex('ext_ref_id',n,rng)
        if not updating:
            d['ext_src_id']=np.array([11000,11004],dtype=np.int64)[rank]
            d['ext_entity_name']=np.array(['prescreenentry','applicationid'],dtype=object)[rank]
    elif t=='lc20_ps.ps_prescreen_entry':
        event('effective_start_d',base,3,1); event('first_look_up_d',base,3,EVENT_LAG_DAYS)
        event('last_look_up_d',d['first_look_up_d'],2,EVENT_LAG_DAYS); event('used_d',d['last_look_up_d'],1,EVENT_LAG_DAYS)
    elif t=='lc20_ps.ps_prescreen_ext_ref':
        if 'ext_ref_id' not in skip: d['ext_ref_id']=unique.hex('ext_ref_id',n,rng)
    elif t=='lc20_uw_processing.uw_process':
        for c in ['subject_ref_id','subject_src_id']:
            if c not in skip: d[c]=unique.hex(c,n,rng)
        event('status_d',base,3,EVENT_LAG_DAYS); event('decision_status_d',d['status_d'],2,EVENT_LAG_DAYS); event('expiration_d',d['decision_status_d'],1,EVENT_LAG_DAYS)
    elif t=='lc20_uw_processing.uw_process_run': pass
    elif t=='lc20_uw_processing.uw_process_run_ext_ref':
        for c in ['ext_ref_id','ext_src_id']:
            if c not in skip: d[c]=unique.hex(c,n,rng)
    for c,pct in sp['nulls'].items():
        if pct!=1. and c not in skip:
            vals=np.asarray(d[c],dtype=object); vals[rng.random(n)<pct]=None; d[c]=vals
    df=coerce(pd.DataFrame({c:d[c] for c,k in COLS[t]}),t); validate_batch(df,t,win,parent_secs,updating)
    return df

class MultipartStream:
    """Holds at most a multipart part plus one generation batch."""
    def __init__(self,key):
        import boto3
        self.client=boto3.client('s3'); self.key=key; self.buf=bytearray(); self.parts=[]; self.closed=False
        self.part_size=int(os.getenv('LC20_S3_PART_MB','64'))*1024*1024
        if self.part_size<5*1024*1024: raise ValueError('S3 multipart parts must be at least 5 MiB')
        self.uid=self.client.create_multipart_upload(Bucket=S3_BUCKET,Key=key)['UploadId']
    def write(self,data):
        if isinstance(data,str): data=data.encode('utf8')
        self.buf.extend(data)
        while len(self.buf)>=self.part_size:
            part=bytes(self.buf[:self.part_size]); del self.buf[:self.part_size]; self.upload(part)
    def upload(self,data):
        number=len(self.parts)+1
        if number>10000: raise ValueError('S3 multipart limit: increase LC20_S3_PART_MB')
        resp=self.client.upload_part(Bucket=S3_BUCKET,Key=self.key,UploadId=self.uid,PartNumber=number,Body=data)
        self.parts.append({'PartNumber':number,'ETag':resp['ETag']})
    def finish(self):
        if self.buf: self.upload(bytes(self.buf)); self.buf.clear()
        self.client.complete_multipart_upload(Bucket=S3_BUCKET,Key=self.key,UploadId=self.uid,MultipartUpload={'Parts':self.parts}); self.closed=True
    def abort(self):
        if not self.closed: self.client.abort_multipart_upload(Bucket=S3_BUCKET,Key=self.key,UploadId=self.uid)

class OutputWriter:
    def __init__(self,t,prefix,delta=False):
        self.t=t; self.delta=delta; self.n=0; self.header=True; self.pw=None
        sch,tab=t.split('.'); self.key=f'{prefix}{sch}/{tab}.{OUTPUT_FORMAT}'
        if STORAGE=='local':
            self.target=Path(LOCAL_DIR)/self.key; self.target.parent.mkdir(parents=True,exist_ok=True)
            self.tmp=self.target.with_name(self.target.name+'.inprogress'); self.f=open(self.tmp,'wb')
        elif OUTPUT_FORMAT=='csv': self.f=MultipartStream(self.key)
        else:
            fd,name=tempfile.mkstemp(suffix='.parquet',dir=WORK_DIR); os.close(fd); self.tmp=Path(name); self.f=open(self.tmp,'wb')
    def write(self,df,op=None):
        out=output_frame(df,self.t)
        if self.delta: out['_operation']=op if op is not None else pd.Series(dtype=object)
        if OUTPUT_FORMAT=='csv':
            with warnings.catch_warnings():
                warnings.simplefilter('ignore',RuntimeWarning); data=out.to_csv(index=False,header=self.header,lineterminator='\n').encode('utf8')
            self.f.write(data); self.header=False
        else:
            fields=[pa.field(c,pa.int64() if k=='i' else pa.float64() if k=='f' else pa.string()) for c,k in COLS[self.t]]
            if self.delta: fields.append(pa.field('_operation',pa.string()))
            table=pa.Table.from_pandas(out,schema=pa.schema(fields),preserve_index=False)
            if self.pw is None: self.pw=pq.ParquetWriter(self.f,table.schema,compression='snappy')
            self.pw.write_table(table,row_group_size=BATCH_ROWS)
        self.n+=len(df)
    def close(self):
        if self.header and self.pw is None: self.write(empty_frame(self.t))
        if self.pw: self.pw.close()
        if isinstance(self.f,MultipartStream): self.f.finish()
        else:
            self.f.close()
            if STORAGE=='local': os.replace(self.tmp,self.target)
            else:
                import boto3
                boto3.client('s3').upload_file(str(self.tmp),S3_BUCKET,self.key); self.tmp.unlink()
    def abort(self):
        if isinstance(self.f,MultipartStream): self.f.abort()
        else:
            self.f.close()
            if self.tmp.exists(): self.tmp.unlink()

def empty_frame(t):
    return pd.DataFrame({c:pd.Series(dtype='int64' if k=='i' else 'float64' if k=='f' else 'datetime64[ns]' if k in ('t','d') else object) for c,k in COLS[t]})

class KeyCache:
    def __init__(self,path): self.path=Path(path); self.writer=None
    def write(self,df):
        table=pa.Table.from_pandas(df[['id','created_d']],preserve_index=False,schema=pa.schema([('id',pa.string()),('created_d',pa.timestamp('ns'))]))
        if self.writer is None: self.writer=pq.ParquetWriter(self.path,table.schema,compression='snappy')
        self.writer.write_table(table,row_group_size=BATCH_ROWS)
    def close(self):
        if self.writer: self.writer.close()
        elif not self.path.exists(): pq.write_table(pa.table({'id':pa.array([],type=pa.string()),'created_d':pa.array([],type=pa.timestamp('ns'))}),self.path)
    def batches(self,size):
        for b in pq.ParquetFile(self.path).iter_batches(batch_size=size): yield b.to_pandas()
    def remove(self):
        if self.path.exists(): self.path.unlink()

CHILDREN={t:[c for c in ORDER if SPECS[c]['fk'] and SPECS[c]['fk'][1]==t] for t in ORDER}
def child_batches(t,cache,rng):
    _,parent,avg,mn,mx=SPECS[t]['fk']
    for p in cache.batches(max(1,BATCH_ROWS//mn)):
        ids=np.repeat(p.id.to_numpy(),mn); secs=np.repeat(p.created_d.astype('int64').to_numpy()//10**9,mn)
        ranks=np.tile(np.arange(mn),len(p)); perm=rng.permutation(len(ids))
        yield ids[perm],secs[perm],ranks[perm]

def manifest_write(prefix,report):
    data=json.dumps(report,indent=2).encode('utf8'); key=prefix+'manifest.json'
    if STORAGE=='local':
        path=Path(LOCAL_DIR)/key; path.parent.mkdir(parents=True,exist_ok=True)
        temp=path.with_suffix('.json.inprogress'); temp.write_bytes(data); os.replace(temp,path); return str(path)
    import boto3
    boto3.client('s3').put_object(Bucket=S3_BUCKET,Key=key,Body=data,ContentType='application/json')
    return f's3://{S3_BUCKET}/{key}'

def run_full():
    win={'start':ts(START_DATE),'end':ts(END_DATE)}; counts=counts_full(); preflight(counts,win)
    report={'mode':'full','seed':SEED,'format':OUTPUT_FORMAT,'batch_rows':BATCH_ROWS,'tables':{}}
    caches={}; remaining={t:len(CHILDREN[t]) for t in ORDER}
    with tempfile.TemporaryDirectory(prefix='full-',dir=WORK_DIR) as work:
        for i,t in enumerate(ORDER):
            rng=np.random.default_rng(np.random.SeedSequence([SEED,i,0])); unique=Uniqueness(t,work,win['start'],win['end'])
            sink=OutputWriter(t,S3_PREFIX); stats=Statistics(t); cache=KeyCache(Path(work)/f'keys-{i}.parquet') if CHILDREN[t] else None
            try:
                if SPECS[t]['fk']:
                    parent=SPECS[t]['fk'][1]; iterator=((len(ids),ids,secs,rank) for ids,secs,rank in child_batches(t,caches[parent],rng))
                else: iterator=((min(BATCH_ROWS,counts[t]-start),None,None,None) for start in range(0,counts[t],BATCH_ROWS))
                for n,ids,secs,rank in iterator:
                    df=generate(t,n,rng,win,unique,ids,secs,rank); stats.add(df); sink.write(df)
                    if cache: cache.write(df)
                if stats.n!=counts[t]: raise AssertionError('Exact output count failed')
                if cache: cache.close(); caches[t]=cache
                sink.close(); report['tables'][t]=stats.finish()
                for w in report['tables'][t]['warnings']: print('WARN:',w)
                print(f'FULL {t}: {stats.n:,} rows (globally unique, batch validated)')
            except Exception: sink.abort(); raise
            finally: unique.close()
            if SPECS[t]['fk']:
                parent=SPECS[t]['fk'][1]; remaining[parent]-=1
                if remaining[parent]==0: caches.pop(parent).remove()
    report['validation']='passed'; print('Manifest:',manifest_write(S3_PREFIX,report)); return report

def input_batches(t):
    sch,tab=t.split('.'); key=f'{S3_PREFIX}{sch}/{tab}.{OUTPUT_FORMAT}'
    if OUTPUT_FORMAT=='csv':
        if STORAGE=='local': stream=open(Path(LOCAL_DIR)/key,'rb')
        else:
            import boto3
            stream=boto3.client('s3').get_object(Bucket=S3_BUCKET,Key=key)['Body']
        try:
            for df in pd.read_csv(stream,dtype=str,keep_default_na=False,na_values=[''],chunksize=BATCH_ROWS): yield coerce(df,t)
        finally: stream.close()
    else:
        if STORAGE=='local': path=Path(LOCAL_DIR)/key; remove=False
        else:
            import boto3
            fd,name=tempfile.mkstemp(suffix='.parquet',dir=WORK_DIR); os.close(fd); path=Path(name); remove=True
            boto3.client('s3').download_file(S3_BUCKET,key,str(path))
        try:
            for b in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS): yield coerce(b.to_pandas(),t)
        finally:
            if remove: path.unlink()

def qi(s): return '"'+s.replace('"','""')+'"'
def sql_schema(t):
    types={'s':'VARCHAR','i':'BIGINT','f':'DOUBLE','t':'TIMESTAMP','d':'TIMESTAMP'}
    return ','.join(qi(c)+' '+types[k] for c,k in COLS[t])
def db_append(con,table,df):
    con.register('_batch',pa.Table.from_pandas(df,preserve_index=False))
    try: con.execute(f'INSERT INTO {table} SELECT * FROM _batch')
    finally: con.unregister('_batch')
def db_batches(con,query):
    cursor=con.cursor()
    try:
        reader=cursor.execute(query).fetch_record_batch(BATCH_ROWS)
        for batch in reader:
            df=batch.to_pandas()
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]): df[c]=df[c].astype('datetime64[ns]')
            yield df
    finally: cursor.close()

def db_validate(con,table,t,parent_table=None):
    sp=SPECS[t]; tests=[]; names=[]
    for c,k in COLS[t]:
        if c not in sp['nulls']: tests.append(f'count(*)-count({qi(c)})'); names.append(c+':required')
    for c in sp['unique']: tests.append(f'count({qi(c)})-count(DISTINCT {qi(c)})'); names.append(c+':unique')
    for c,(lo,hi) in sp['ranges'].items(): tests.append(f'count(*) FILTER (WHERE {qi(c)} < {lo} OR {qi(c)} > {hi})'); names.append(c+':range')
    for b,a in [('created_d','modified_d')]+sp['temporal']: tests.append(f'count(*) FILTER (WHERE {qi(a)} < {qi(b)})'); names.append(b+':temporal')
    vals=con.execute('SELECT '+','.join(tests)+f' FROM {table}').fetchone(); errors=[name for name,v in zip(names,vals) if v]
    if errors: raise AssertionError(t+': '+','.join(errors))
    if parent_table and sp['fk']:
        c,p,avg,mn,mx=sp['fk']
        missing=con.execute(f'SELECT count(*) FROM {table} c ANTI JOIN {parent_table} p ON c.{qi(c)}=p.id').fetchone()[0]
        bad=con.execute(f'SELECT count(*) FROM (SELECT p.id,count(c.id) n FROM {parent_table} p LEFT JOIN {table} c ON p.id=c.{qi(c)} GROUP BY p.id HAVING count(c.id)<{mn} OR count(c.id)>{mx})').fetchone()[0]
        if missing or bad: raise AssertionError(t+': FK/cardinality')
    if t=='lc20_das.das_origination_ext_ref':
        bad=con.execute(f"SELECT count(*) FROM (SELECT origination_id FROM {table} GROUP BY origination_id HAVING count(*)!=2 OR count(*) FILTER (WHERE ext_src_id=11000 AND ext_entity_name='prescreenentry')!=1 OR count(*) FILTER (WHERE ext_src_id=11004 AND ext_entity_name='applicationid')!=1)").fetchone()[0]
        if bad: raise AssertionError('DAS source/entity pairing')

def run_incremental():
    counts={}; low=ts(START_DATE); max_ts=None
    for t in ORDER:
        count=0
        for df in input_batches(t):
            validate_batch(df,t); count+=len(df)
            for c,k in COLS[t]:
                if k=='t' and len(df):
                    lo=int(df[c].min().value//10**9); hi=int(df[c].max().value//10**9)
                    low=min(low,lo); max_ts=hi if max_ts is None else max(max_ts,hi)
        counts[t]=count
    if max_ts is None: raise ValueError('Incremental source has no timestamps; use full generation first')
    start=(max_ts//DAY+1)*DAY; half=(INCR_WINDOW_DAYS//2)*DAY
    if half<5*DAY: raise ValueError('Incremental half-window must leave the original four-day timestamp headroom')
    win_up={'start':start,'end':start+half-1}; win_ins={'start':start+half,'end':start+2*half}
    inserts={}
    for t in ORDER:
        sp=SPECS[t]
        inserts[t]=0 if t in GOLDEN_TABLES else int(round(counts[t]*INSERT_PCT/100)) if sp['fk'] is None else int(round(inserts[sp['fk'][1]]*sp['fk'][2]))
    preflight(counts,{'start':low,'end':max_ts},inserts)
    preflight({t:counts[t]+inserts[t] for t in ORDER},win_ins,inserts)
    for t in ORDER:
        if int(round(counts[t]*UPDATE_PCT/100))>win_up['end']-win_up['start']: raise ValueError(t+': updates exceed unique modified_d window capacity')
    report={'mode':'incremental','output':INCREMENTAL_OUTPUT,'seed':SEED,'format':OUTPUT_FORMAT,'tables':{}}
    idx={t:i for i,t in enumerate(ORDER)}; caches={}; remaining={t:len(CHILDREN[t]) for t in ORDER}
    with tempfile.TemporaryDirectory(prefix='incremental-',dir=WORK_DIR) as work:
        con=duckdb.connect(str(Path(work)/'incremental.duckdb'))
        con.execute(f"SET memory_limit='{DB_MEMORY}'"); con.execute(f'SET threads={DB_THREADS}')
        spill=Path(work)/'spill'; spill.mkdir(); con.execute('SET temp_directory=?',[str(spill)]); con.execute('SET preserve_insertion_order=false')
        try:
            for i,t in enumerate(ORDER):
                sp=SPECS[t]; rng=np.random.default_rng(np.random.SeedSequence([SEED,i,1])); con.execute('CREATE TABLE cur ('+sql_schema(t)+')')
                for df in input_batches(t): db_append(con,'cur',df)
                parent_i=idx[sp['fk'][1]] if sp['fk'] else None
                db_validate(con,'cur',t,f'original_keys_{parent_i}' if parent_i is not None else None)
                if CHILDREN[t]: con.execute(f'CREATE TABLE original_keys_{i} AS SELECT id,created_d FROM cur')
                if t in GOLDEN_TABLES: con.execute(f'CREATE TABLE deleted_ids_{i} AS SELECT id FROM cur WHERE false')
                elif sp['fk'] is None:
                    nd=int(round(counts[t]*DELETE_PCT/100)); con.execute(f'CREATE TABLE deleted_ids_{i} AS SELECT id FROM cur USING SAMPLE {nd} ROWS (reservoir,{SEED+i})')
                else:
                    col=sp['fk'][0]; con.execute(f'CREATE TABLE deleted_ids_{i} AS SELECT c.id FROM cur c JOIN deleted_ids_{parent_i} p ON c.{qi(col)}=p.id')
                nd=con.execute(f'SELECT count(*) FROM deleted_ids_{i}').fetchone()[0]
                delta_mode=INCREMENTAL_OUTPUT=='delta'; sink=OutputWriter(t,INCREMENTAL_PREFIX,delta=delta_mode); stats=Statistics(t)
                try:
                    if delta_mode:
                        for df in db_batches(con,f'SELECT c.* FROM cur c JOIN deleted_ids_{i} d ON c.id=d.id'): sink.write(df,'DELETE')
                    con.execute(f'DELETE FROM cur USING deleted_ids_{i} d WHERE cur.id=d.id'); unique=Uniqueness(t,work,low,win_ins['end'])
                    try:
                        for df in db_batches(con,'SELECT * FROM cur'): unique.seed(df)
                        survivors=counts[t]-nd; nu=0 if t in GOLDEN_TABLES else int(round(survivors*UPDATE_PCT/100))
                        con.execute(f'CREATE TABLE selected_ids AS SELECT id FROM cur USING SAMPLE {nu} ROWS (reservoir,{SEED+100+i})')
                        con.execute('CREATE TABLE replacements ('+sql_schema(t)+')')
                        for old in db_batches(con,'SELECT c.* FROM cur c JOIN selected_ids s ON c.id=s.id'):
                            new=generate(t,len(old),rng,win_up,unique,old=old); db_append(con,'replacements',new)
                            if delta_mode: sink.write(new,'UPDATE')
                        con.execute('DELETE FROM cur USING selected_ids s WHERE cur.id=s.id'); con.execute('INSERT INTO cur SELECT * FROM replacements')
                        con.execute('DROP TABLE selected_ids'); con.execute('DROP TABLE replacements')
                        for df in db_batches(con,'SELECT * FROM cur'):
                            validate_batch(df,t); stats.add(df)
                            if not delta_mode: sink.write(df)
                        cache=KeyCache(Path(work)/f'inserted-keys-{i}.parquet') if CHILDREN[t] else None
                        if sp['fk']: iterator=((len(ids),ids,secs,rank) for ids,secs,rank in child_batches(t,caches[sp['fk'][1]],rng))
                        else: iterator=((min(BATCH_ROWS,inserts[t]-offset),None,None,None) for offset in range(0,inserts[t],BATCH_ROWS))
                        actual_ins=0
                        for n,ids,secs,rank in iterator:
                            new=generate(t,n,rng,win_ins,unique,ids,secs,rank); stats.add(new); sink.write(new,'INSERT' if delta_mode else None); actual_ins+=n
                            if cache: cache.write(new)
                        if actual_ins!=inserts[t]: raise AssertionError('Incremental insert count mismatch')
                        if cache: cache.close(); caches[t]=cache
                        if stats.n!=survivors+inserts[t]: raise AssertionError('Merged row count mismatch')
                        # Source cur was exactly validated. Replacement draws are
                        # reserved against all survivors; id, created_d, FK and
                        # nonmutables stay unchanged. New child keys come only
                        # from newly inserted parent keys, with exact multiplicity.
                        sink.close(); report['tables'][t]=stats.finish()
                        report['tables'][t].update(deleted=nd,updated=nu,inserted=actual_ins,output_rows=sink.n)
                        for w in report['tables'][t]['warnings']: print('WARN:',w)
                        print(f'INCR {t}: delete={nd} update={nu} insert={actual_ins} merged={stats.n}')
                    finally: unique.close()
                except Exception: sink.abort(); raise
                con.execute('DROP TABLE cur')
                if parent_i is not None:
                    parent=sp['fk'][1]; remaining[parent]-=1
                    if remaining[parent]==0:
                        caches.pop(parent).remove(); con.execute(f'DROP TABLE original_keys_{parent_i}'); con.execute(f'DROP TABLE deleted_ids_{parent_i}')
                if not CHILDREN[t]: con.execute(f'DROP TABLE deleted_ids_{i}')
        finally: con.close()
    report['validation']='passed'; print('Manifest:',manifest_write(INCREMENTAL_PREFIX,report)); return report

def main():
    if STORAGE not in {'local','s3'} or OUTPUT_FORMAT not in {'csv','parquet'} or MODE not in {'full','incremental'} or INCREMENTAL_OUTPUT not in {'merged','delta'}: raise ValueError('Invalid configuration')
    if BATCH_ROWS<2 or SCALE_FACTOR<0: raise ValueError('Invalid batch size or scale')
    if not 0<=DELETE_PCT<=100 or not 0<=UPDATE_PCT<=100 or INSERT_PCT<0: raise ValueError('Invalid incremental percentages')
    Path(WORK_DIR).mkdir(parents=True,exist_ok=True); init_pools(SEED)
    return run_full() if MODE=='full' else run_incremental()

# Independent small checks. Only these test routines collect whole datasets.
def small_read(prefix):
    dfs={}
    for t in ORDER:
        sch,tab=t.split('.'); raw=pd.read_csv(Path(LOCAL_DIR)/prefix/sch/(tab+'.csv'),dtype=str,keep_default_na=False,na_values=[''])
        extra=raw.pop('_operation') if '_operation' in raw else None; df=coerce(raw,t)
        if extra is not None: df['_operation']=extra
        dfs[t]=df
    return dfs

def independent_check(dfs,expected=None):
    for t in ORDER:
        df=dfs[t]; sp=SPECS[t]; validate_batch(df,t)
        assert list(df.columns)==[c for c,k in COLS[t]]
        if expected is not None: assert len(df)==expected[t],(t,len(df),expected[t])
        for c in sp['unique']: assert not df[c].duplicated().any(),(t,c)
        if sp['fk']:
            col,parent,avg,mn,mx=sp['fk']; p=dfs[parent]; assert df[col].isin(p.id).all()
            assert df.groupby(col).size().reindex(p.id,fill_value=0).between(mn,mx).all()
        if t=='lc20_das.das_origination_ext_ref':
            assert (df.groupby('origination_id').ext_src_id.nunique()==2).all()
            assert (((df.ext_src_id==11000)&(df.ext_entity_name=='prescreenentry'))|((df.ext_src_id==11004)&(df.ext_entity_name=='applicationid'))).all()
    return True

def self_test():
    global STORAGE,LOCAL_DIR,S3_PREFIX,INCREMENTAL_PREFIX,OUTPUT_FORMAT,MODE,SCALE_FACTOR,BATCH_ROWS,INCREMENTAL_OUTPUT,ROW_COUNT_OVERRIDES
    STORAGE='local'; LOCAL_DIR='/tmp/lc20_optimized/data'; OUTPUT_FORMAT='csv'; S3_PREFIX='full/'; INCREMENTAL_PREFIX='incremental_merged/'
    MODE='full'; SCALE_FACTOR=1.; BATCH_ROWS=137; ROW_COUNT_OVERRIDES={}
    main(); original=small_read(S3_PREFIX); independent_check(original,counts_full())
    MODE='incremental'; INCREMENTAL_OUTPUT='merged'; main(); merged=small_read(INCREMENTAL_PREFIX)
    expected={t:counts_full()[t]+int(round(counts_full()[t]*INSERT_PCT/100))-int(round(counts_full()[t]*DELETE_PCT/100)) for t in ORDER}
    independent_check(merged,expected)
    INCREMENTAL_OUTPUT='delta'; INCREMENTAL_PREFIX='incremental_delta/'; delta_report=main(); delta=small_read(INCREMENTAL_PREFIX)
    for t in ORDER:
        old=original[t].set_index('id'); d=delta[t]; deletes=d.loc[d._operation=='DELETE','id']
        updates=d[d._operation=='UPDATE'].drop(columns='_operation'); ins=d[d._operation=='INSERT'].drop(columns='_operation')
        assert set(d._operation)<={'DELETE','UPDATE','INSERT'}
        assert len(deletes)==delta_report['tables'][t]['deleted']; assert len(updates)==delta_report['tables'][t]['updated']; assert len(ins)==delta_report['tables'][t]['inserted']
        assert not d.id.duplicated().any()
        fkc=SPECS[t]['fk'][0] if SPECS[t]['fk'] else None
        for c in ['created_d',fkc]+SPECS[t]['nonmut']:
            if c is not None and len(updates): pd.testing.assert_series_equal(updates.set_index('id')[c].sort_index(),old.loc[updates.id,c].sort_index(),check_names=False)
        assert np.array_equal(updates.version.to_numpy(),np.minimum(old.loc[updates.id,'version'].to_numpy()+1,SPECS[t]['ranges']['version'][1]))
        cur=old.drop(index=deletes)
        if len(updates):
            replacement=updates.set_index('id'); cur=cur.drop(index=replacement.index); cur=pd.concat([cur,replacement])
        replay=pd.concat([cur.reset_index(),ins],ignore_index=True)[[c for c,k in COLS[t]]]
        pd.testing.assert_frame_equal(replay.sort_values('id').reset_index(drop=True),merged[t].sort_values('id').reset_index(drop=True))
    rejected=False
    try: preflight({t:n*100000 for t,n in counts_full().items()},{'start':ts(START_DATE),'end':ts(END_DATE)})
    except ValueError as exc: rejected='offer_amt' in str(exc) and 'timestamp capacity' in str(exc)
    assert rejected,'Infeasible-scale guard was not triggered'
    test={'status':'passed','roots_tested':1000,'full_rows':sum(len(df) for df in original.values()),'merged_rows':sum(len(df) for df in merged.values()),'delta_rows':sum(len(df) for df in delta.values()),'batch_rows':BATCH_ROWS,
     'checks':['schema','required fields','global uniqueness across batches','ranges','enum membership','DOB/SSN/name/email formats','temporal ordering','exact FK cardinality','DAS pairs','cascade deletes','immutable update fields','version ceilings','delta replay equals merged','infeasible-scale rejection'],
     'note':'No large-scale or live-S3 run was performed; statistical deviations remain warnings, as in the source.'}
    Path('/tmp/lc20_optimized').mkdir(parents=True,exist_ok=True)
    Path('/tmp/lc20_optimized/validation_report.json').write_text(json.dumps(test,indent=2))
    print(json.dumps(test,indent=2)); return test

if __name__=='__main__':
    if SELF_TEST: self_test()
    else: main()
