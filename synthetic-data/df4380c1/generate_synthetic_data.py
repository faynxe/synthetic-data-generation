"""LC20 bounded-memory generator.
Run: python lc20_optimized.py
Configuration is via LC20_* environment variables; CSV is the default.
For 100M total rows at the original table ratios: LC20_SCALE_FACTOR=6250.
At fixed two-decimal precision ps_prescreen_entry.offer_amt has 9,980,678
possible unique values; second-resolution timestamps also have finite capacity.
Consequently arbitrary 1B-row scaling of this schema is mathematically impossible
without explicitly authorizing changes to precision/date bounds or cardinalities.
This script rejects impossible requests rather than producing invalid data.
LC20_BATCH_ROWS=50000, LC20_WORKERS=1 (increase on standard multicore hosts);
LC20_DB_MEMORY=1GB, LC20_DB_THREADS=2, LC20_SCRATCH_DIR=/large/local/volume.
LC20_STORAGE=s3 retains the original bucket/prefix; use local for offline work.
LC20_MODE=incremental and LC20_INCREMENTAL_OUTPUT=delta or merged retain the
original semantics and read the same one-file-per-table baseline layout.
Increase workers only with sufficient aggregate memory and scratch disk.
Random streams are reproducible for fixed seed, batch size and configuration;
row-for-row equality with the original monolithic generator is not implied.
Requires numpy, pandas, Faker, pyarrow, duckdb; boto3 only for S3.
The test harness uses 1,000 rows per root (2,000 DAS external references).
No rules, precision, date windows or cardinalities are relaxed for scaling.
Full generation parallelizes independent root families, never individual rows.
Incremental execution uses a spilling, disk-backed DuckDB database.
Scratch disk must hold staged output, uniqueness registries, and spill files;
S3 uploads use multipart transfer, not an in-memory CSV buffer.
Outputs are only published after exact global validation. A manifest is written last.
"""
import os, io, math, json, uuid, shutil, sqlite3, tempfile, warnings
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from collections import Counter
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import duckdb
from faker import Faker

SEED = int(os.getenv('LC20_SEED','42'))
SCALE_FACTOR = float(os.getenv('LC20_SCALE_FACTOR','1'))
S3_BUCKET = os.getenv('LC20_S3_BUCKET','mm-fsi-fix')
S3_PREFIX = os.getenv('LC20_S3_PREFIX','lc/synthetic_data_1/runs/20261011-223023/')
INCREMENTAL_PREFIX = os.getenv('LC20_INCREMENTAL_PREFIX',S3_PREFIX+'incremental/')
STORAGE = os.getenv('LC20_STORAGE','s3')
LOCAL_DIR = os.getenv('LC20_LOCAL_DIR','/tmp/synthetic_out')
SCRATCH_DIR = os.getenv('LC20_SCRATCH_DIR',LOCAL_DIR+'/.scratch')
OUTPUT_FORMAT = os.getenv('LC20_OUTPUT_FORMAT','csv')
MODE = os.getenv('LC20_MODE','full')
INCREMENTAL_OUTPUT = os.getenv('LC20_INCREMENTAL_OUTPUT','merged')
INSERT_PCT = float(os.getenv('LC20_INSERT_PCT','10'))
UPDATE_PCT = float(os.getenv('LC20_UPDATE_PCT','5'))
DELETE_PCT = float(os.getenv('LC20_DELETE_PCT','2'))
START_DATE = os.getenv('LC20_START_DATE','2025-01-01')
END_DATE = os.getenv('LC20_END_DATE','2025-11-28 23:59:59')
INCR_WINDOW_DAYS = int(os.getenv('LC20_INCR_WINDOW_DAYS','20'))
CHILD_LAG_DAYS, MOD_LAG_DAYS, EVENT_LAG_DAYS = 7,60,30
FIRST_NAME_POOL, LAST_NAME_POOL = 400,550
DOB_MIN, DOB_MAX = '1934-12-07','2007-11-17'
MEMBER_REF_MIN, MEMBER_REF_MAX = 10836,997603725
SSN_LO, SSN_HI = 100_000_000,899_999_999
EMAIL_DOMAINS = ['gmail.com','yahoo.com','outlook.com','hotmail.com','aol.com','icloud.com','example.com']
KEY_NAME_FEK = 'application-store_first_name_fek'
KEY_EMAIL_FEK = 'application-store_email_address_fek'
DIST_TOLERANCE, STRICT, DAY = .06,True,86400
BATCH_ROWS = int(os.getenv('LC20_BATCH_ROWS','50000'))
WORKERS = int(os.getenv('LC20_WORKERS','1'))
DB_MEMORY = os.getenv('LC20_DB_MEMORY','1GB')
DB_THREADS = int(os.getenv('LC20_DB_THREADS','2'))
SELF_TEST = os.getenv('LC20_SELF_TEST','0') == '1'
A,D,P,U = 'lc20_as.','lc20_das.','lc20_ps.','lc20_uw_processing.'
ROOT_ROW_COUNTS = {A+'as_application':1000,D+'das_origination':1000,P+'ps_prescreen_entry':1000,U+'uw_process':1000}
GOLDEN_TABLES = json.loads(os.getenv('LC20_GOLDEN_TABLES','{}'))
ROW_COUNT_OVERRIDES = json.loads(os.getenv('LC20_ROW_COUNT_OVERRIDES','{}'))
ORDER = [A+x for x in ['as_application','as_applicant','as_applicant_detail','as_application_certificate_of_d','as_application_checking_account','as_application_saving_account','as_email_address']] + [D+x for x in ['das_origination','das_member_origination','das_origination_ext_ref']] + [P+x for x in ['ps_prescreen_entry','ps_prescreen_ext_ref']] + [U+x for x in ['uw_process','uw_process_run','uw_process_run_ext_ref']]
COMMON = 'id:s created_d:t modified_d:t version:i '
def spec(t, cols, idb=16, depth=0, fk=None, nonmut=(), unique=(), ranges=None, nulls=None, temporal=()):
    return dict(cols=COMMON+cols,idb=idb,depth=depth,fk=fk,nonmut=list(nonmut),unique=['id','created_d','modified_d']+list(unique),ranges=ranges or {},nulls=nulls or {},temporal=list(temporal))
def fk(col,parent,count=1): return (col,parent,float(count),count,count)
SPECS = {
A+'as_application':spec('', 'application_d:t product_type_cd:s application_type_cd:s',nonmut=['application_d'],unique=['application_d'],ranges={'version':(0,1)},nulls={'product_type_cd':.904,'application_type_cd':.096}),
A+'as_applicant':spec('', 'application_id:s primary_applicant_flg:i sms_marketing_opt_in_flg:i marketing_attribution_cd:f',depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(0,1),'primary_applicant_flg':(1,1),'sms_marketing_opt_in_flg':(0,1)},nulls={'marketing_attribution_cd':1.}),
A+'as_applicant_detail':spec('', 'applicant_id:s source_cd:s first_name:s first_name_keyn:s first_name_keyv:i last_name:s last_name_keyn:s last_name_keyv:i honorific_cd:f date_of_birth:d date_of_birth_keyn:s date_of_birth_keyv:i ssn:s ssn_keyn:s ssn_keyv:i marital_status_cd:f educational_qualification_cd:f employment_status_cd:s cumulative_job_tenure_months:i annual_income:f annual_additional_income:f annual_income_taxable:f annual_income_non_taxable:f housing_status_cd:s annual_housing_expense:f annual_additional_debt:f fico_range_cd:f fico_min:i fico_max:i citizenship:f',depth=2,fk=fk('applicant_id',A+'as_applicant'),nonmut=['ssn'],unique=['applicant_id','ssn','annual_income','annual_additional_income','annual_income_taxable','annual_income_non_taxable','annual_housing_expense','annual_additional_debt'],ranges={'version':(0,10),'first_name_keyv':(1,1),'last_name_keyv':(1,1),'date_of_birth_keyv':(1,1),'ssn_keyv':(1,1),'cumulative_job_tenure_months':(1,20),'annual_income':(13.5021,9998.64691),'annual_additional_income':(6.20378,9947.23003),'annual_income_taxable':(1.21818,9997.39902),'annual_income_non_taxable':(7.48881,9994.96678),'annual_housing_expense':(23.61788,9996.5062),'annual_additional_debt':(.55862,9982.90525),'fico_min':(1,5),'fico_max':(1,5)},nulls={'honorific_cd':1.,'marital_status_cd':1.,'educational_qualification_cd':1.,'employment_status_cd':.102,'housing_status_cd':.106,'fico_range_cd':1.,'citizenship':1.}),
A+'as_application_certificate_of_d':spec('', 'source_cd:s initial_deposit_amount:i product_code:i application_id:s apy:f term_in_months:i is_joint_flg:i account_type:s',idb=9,depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(0,5),'initial_deposit_amount':(155,99836),'product_code':(6,28),'apy':(3.4,5.),'term_in_months':(6,60),'is_joint_flg':(0,1)}),
A+'as_application_checking_account':spec('', 'application_id:s source_cd:s debit_card_requested_flg:i min_initial_deposit_amount:i initial_deposit_amount:i product_code:i is_joint_flg:i account_type:s',idb=9,depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(0,10),'debit_card_requested_flg':(0,1),'min_initial_deposit_amount':(1,999),'initial_deposit_amount':(3,998),'product_code':(1,53),'is_joint_flg':(0,1)}),
A+'as_application_saving_account':spec('', 'source_cd:s application_id:s initial_deposit_amount:i product_code:i is_joint_flg:i debit_card_requested_flg:i account_type:s',idb=9,depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(1,10),'initial_deposit_amount':(19,9989),'product_code':(9,25),'is_joint_flg':(0,1),'debit_card_requested_flg':(0,1)}),
A+'as_email_address':spec('', 'applicant_id:s source_cd:s email_address:s email_address_keyn:s email_address_keyv:i',depth=2,fk=fk('applicant_id',A+'as_applicant'),nonmut=['email_address'],unique=['applicant_id','email_address'],ranges={'version':(0,20),'email_address_keyv':(1,1)}),
D+'das_origination':spec('', 'origination_guid:s',nonmut=['origination_guid'],unique=['origination_guid'],ranges={'version':(0,0)}),
D+'das_member_origination':spec('', 'origination_id:s member_ref_id:i member_src_id:i member_entity_name:s',depth=1,fk=fk('origination_id',D+'das_origination'),nonmut=['member_ref_id'],unique=['origination_id','member_ref_id'],ranges={'version':(0,0),'member_ref_id':(MEMBER_REF_MIN,MEMBER_REF_MAX),'member_src_id':(1000,1200)}),
D+'das_origination_ext_ref':spec('', 'origination_id:s ext_ref_id:s ext_src_id:i ext_entity_name:s flow_id:s flow_version:f',depth=1,fk=fk('origination_id',D+'das_origination',2),nonmut=['ext_ref_id','ext_src_id','ext_entity_name'],unique=['ext_ref_id'],ranges={'version':(0,5),'ext_src_id':(11000,11004),'flow_version':(0,5)},nulls={'flow_id':.108,'flow_version':.519}),
P+'ps_prescreen_entry':spec('', 'source_cd:s credit_policy_name:s credit_policy_version:i effective_start_d:t effective_end_d:f status_cd:f first_look_up_d:t last_look_up_d:t used_d:t offer_amt:f offer_purpose_cd:s',unique=['effective_start_d','first_look_up_d','last_look_up_d','used_d','offer_amt'],ranges={'version':(1,862),'credit_policy_version':(100,344),'offer_amt':(70.05,99876.82)},nulls={'source_cd':.42,'credit_policy_name':.581,'effective_end_d':1.,'status_cd':1.,'offer_purpose_cd':.406},temporal=[('first_look_up_d','last_look_up_d'),('last_look_up_d','used_d')]),
P+'ps_prescreen_ext_ref':spec('', 'ps_prescreen_entry_id:s ext_src_id:s ext_ref_id:s ext_entity_name:s ext_ref_direction_cd:s',depth=1,fk=fk('ps_prescreen_entry_id',P+'ps_prescreen_entry'),nonmut=['ext_ref_id'],unique=['ps_prescreen_entry_id','ext_ref_id'],ranges={'version':(0,0)},nulls={'ext_ref_direction_cd':.101}),
U+'uw_process':spec('', 'subject_ref_id:s subject_entity_name:s type_cd:s subject_src_id:s status_cd:s product_type_cd:f status_d:t decision_status_cd:s decision_status_d:t decision_phase_cd:s subject_type_cd:s expiration_d:t',nonmut=['subject_ref_id','subject_src_id'],unique=['subject_ref_id','subject_src_id','status_d','decision_status_d','expiration_d'],ranges={'version':(1,10)},nulls={'type_cd':.403,'product_type_cd':1.,'decision_phase_cd':.097},temporal=[('status_d','decision_status_d'),('decision_status_d','expiration_d')]),
U+'uw_process_run':spec('', 'uw_process_id:s decision_phase_cd:s decision_status_cd:s',depth=1,fk=fk('uw_process_id',U+'uw_process'),unique=['uw_process_id'],ranges={'version':(1,10)},nulls={}),
U+'uw_process_run_ext_ref':spec('', 'uw_process_run_id:s ext_ref_id:s ext_src_id:s ext_ref_entity_name:s',depth=2,fk=fk('uw_process_run_id',U+'uw_process_run'),nonmut=['ext_ref_id','ext_src_id'],unique=['uw_process_run_id','ext_ref_id','ext_src_id'],ranges={'version':(1,10)})}
# created_d uniqueness is NOT required for DAS member origination; generation keeps it,
# matching the supplied generator, but validation uses the original constraint list.
SPECS[D+'das_member_origination']['unique'].remove('created_d')
COLS = {t:[tuple(x.split(':')) for x in s['cols'].split()] for t,s in SPECS.items()}
ENUMS = {
A+'as_application':{'version':{0:.532,1:.468},'application_type_cd':{'JOINT_CERTIFICATE_DEPOSIT_ACCOUNT':.281,'JOINT_SAVINGS_ACCOUNT':.2456,'JOINT_CHECKING_ACCOUNT':.2179,'CERTIFICATE_DEPOSIT_ACCOUNT':.1582,'SAVINGS_ACCOUNT':.0531,'CHECKING_ACCOUNT':.0442}},
A+'as_applicant':{'version':{0:.527,1:.473},'sms_marketing_opt_in_flg':{0:.506,1:.494}},
A+'as_applicant_detail':{'version':{0:.104,5:.098,6:.097,4:.096,1:.095,3:.094,10:.09,9:.088,8:.081,2:.079,7:.078},'employment_status_cd':{'OTHER':.1826,'RETIRED':.1804,'UNEMPLOYED':.1771,'EMPLOYED':.1648,'SELFEMPLOYED':.1537,'DISABLED':.1414},'housing_status_cd':{'OTHER':.1801,'OWN':.1723,'NONE':.1689,'RENT':.1633,'ANY':.1588,'MORTGAGE':.1566},'fico_min':{3:.221,5:.211,2:.201,1:.192,4:.175},'fico_max':{5:.231,3:.219,2:.188,1:.181,4:.181}},
A+'as_application_certificate_of_d':{'version':{5:.185,4:.173,3:.171,1:.167,0:.162,2:.142},'product_code':{25:.134,21:.12,14:.115,28:.115,6:.113,22:.112,12:.103,23:.099,26:.089},'is_joint_flg':{0:.528,1:.472}},
A+'as_application_checking_account':{'version':{1:.102,2:.101,9:.095,4:.094,0:.093,3:.09,8:.089,6:.089,10:.087,5:.086,7:.074},'debit_card_requested_flg':{0:.527,1:.473},'product_code':{1:.506,53:.494},'is_joint_flg':{0:.506,1:.494}},
A+'as_application_saving_account':{'version':{1:.115,3:.106,8:.102,10:.101,6:.101,2:.097,7:.097,5:.095,9:.094,4:.092},'product_code':{25:.362,10:.322,9:.316},'is_joint_flg':{0:.527,1:.473},'debit_card_requested_flg':{0:.506,1:.494}},
A+'as_email_address':{'version':{20:.063,12:.058,14:.057,3:.056,4:.053,15:.053,6:.053,5:.052,17:.049,7:.048,18:.048,9:.048,1:.046,10:.044,19:.043,8:.043,11:.041,2:.039,16:.036,13:.035,0:.035}},
D+'das_member_origination':{'member_src_id':{1200:.508,1000:.492},'member_entity_name':{'pfloginuserid':.505,'loginactorid':.495}},
D+'das_origination_ext_ref':{'version':{0:.179,1:.178,5:.167,4:.166,3:.16,2:.15},'flow_id':{'jointCertificateDepositAccountFlow':.1446,'checkingAccountFlow':.1334,'savingsAccountFlow':.1312,'jointCheckingAccountFlow':.1256,'jointSavingsAccountFlow':.1211,'creditInABoxFlow':.1211,'certificateDepositAccountFlow':.1177,'balanceTransferFlow':.1054},'flow_version':{0.:.4096,5.:.1809,4.:.1143,1.:.106,3.:.1019,2.:.0873}},
P+'ps_prescreen_entry':{'source_cd':{'LIGHTBOX_AUTO':.0879,'EASYAPPLY':.0845,'PL_PRE_APPROVAL':.0793,'PL2AUTO':.0776,'PROSPECT':.0776,'PL2BT':.0759,'PL2PL_MB_TOPUP':.0724,'CLEANSWEEP':.069,'PL2PL_CP_TOPUP':.0672,'JOINT_SAVINGS_ACCOUNT':.0655,'PL2PL_TOPUP':.0638,'PLDECLINE2AUTO':.0621,'EXPERIAN_CREDIT_MATCH':.0586,'LIGHTBOX':.0586},'credit_policy_name':{'cleansweep':.3365,'PL':.3365,'personal':.327},'offer_purpose_cd':{'home_improvement':.0926,'credit_card':.0791,'wedding':.0774,'renewable_energy':.0774,'educational':.0741,'debt_consolidation':.0707,'small_business':.0673,'major_purchase':.0673,'not_requested':.0673,'other':.0606,'moving':.0572,'house':.0572,'vacation':.0556,'car':.0522,'medical':.0438}},
P+'ps_prescreen_ext_ref':{'ext_src_id':{'11004':.817,'11001':.053,'11002':.03,'11008':.027,'14000':.026,'1000':.024,'PL2BT':.023},'ext_entity_name':{'applicationid':.3,'actor':.241,'loan':.196,'referrer':.106,'bpsrequestid':.097,'program':.012,'bankaccount':.011,'accounttype':.008,'product':.007,'loanapp':.006,'loginactorid':.006,'cif':.006,'productcode':.004},'ext_ref_direction_cd':{'SOURCE':.8354,'TARGET':.1646}},
U+'uw_process':{'version':{2:.114,5:.112,9:.111,8:.108,10:.099,4:.095,6:.095,3:.091,7:.089,1:.086},'subject_entity_name':{'actor':.359,'applicationid':.326,'address':.315},'type_cd':{'APPLICATION_DECISION':.526,'ADDRESS_VALIDATION':.474},'status_cd':{'SUCCESS':.413,'NEW':.111,'ACTIVE':.104,'FAILURE':.102,'EXPIRED':.1,'WITHDRAWN':.092,'CANCELED':.078},'decision_status_cd':{'SUCCESS':.443,'PENDING':.168,'ERROR':.102,'MORE_INFO':.101,'FAILURE':.096,'IN_PROGRESS':.09},'decision_phase_cd':{'DRAW_VERIFICATION':.1406,'FULL_DECISION':.1362,'FULFILLMENT':.1285,'ADDRESS_VALIDATION':.1285,'UNKNOWN':.1218,'BE':.1218,'FE':.1196,'BANK_ACCOUNT':.103},'subject_type_cd':{'JOINT_CERTIFICATE_DEPOSIT_ACCOUNT':.236,'CERTIFICATE_DEPOSIT_ACCOUNT':.16,'JOINT_CHECKING_ACCOUNT':.156,'SAVINGS_ACCOUNT':.111,'JOINT_SAVINGS_ACCOUNT':.093,'CHECKING_ACCOUNT':.071,'ADDRESS_BUSINESS':.064,'ADDRESS':.055,'CS_ORDER':.054}},
U+'uw_process_run':{'version':{2:.112,1:.109,9:.109,4:.103,5:.102,6:.097,7:.097,3:.095,8:.089,10:.087},'decision_phase_cd':{'ADDRESS_VALIDATION':.257,'FULFILLMENT':.254,'FULL_DECISION':.247,'BANK_ACCOUNT':.242},'decision_status_cd':{'PENDING':.191,'MORE_INFO':.169,'SUCCESS':.168,'FAILURE':.167,'IN_PROGRESS':.159,'ERROR':.146}},
U+'uw_process_run_ext_ref':{'version':{1:.129,4:.11,7:.107,3:.106,9:.101,8:.1,2:.094,10:.092,5:.081,6:.08},'ext_ref_entity_name':{'journeyapplicationtoken':.122,'loginactorid':.119,'narmiuuid':.116,'offerid':.114,'entitytoken':.113,'bankaccount':.112,'applicantid':.103,'prescreenentry':.102,'cif':.099}}
}
POOLS = {}
def ts(s): return int(pd.Timestamp(s).timestamp())
def init_pools(seed):
    fake=Faker('en_US'); fake.seed_instance(seed)
    def pool(fn,size,lo,hi):
        out={}
        for _ in range(50000):
            v=fn()
            if lo<=len(v)<=hi and v.isalpha(): out[v]=None
            if len(out)==size: break
        if not out: raise ValueError('Empty name pool')
        return np.array(list(out),dtype=object)
    POOLS['first']=pool(fake.first_name,FIRST_NAME_POOL,3,11)
    POOLS['last']=pool(fake.last_name,LAST_NAME_POOL,2,11)

def pick(rng,n,freq):
    keys=np.array(list(freq)); p=np.array(list(freq.values()),dtype=float); p/=p.sum()
    return keys[rng.choice(len(keys),n,p=p)]
def random_hex(rng,n,nbytes):
    # One hex conversion for the entire batch; no per-row Python byte loop.
    raw=rng.integers(0,256,(n,nbytes),dtype=np.uint8)
    return np.char.decode(np.frombuffer(raw.tobytes().hex().encode('ascii'),dtype=f'S{2*nbytes}'),'ascii').astype(object)

def target_counts():
    counts={}
    for t in ORDER:
        if t in GOLDEN_TABLES: n=int(GOLDEN_TABLES[t])
        elif t in ROW_COUNT_OVERRIDES: n=int(ROW_COUNT_OVERRIDES[t])
        elif t in ROOT_ROW_COUNTS: n=round(ROOT_ROW_COUNTS[t]*SCALE_FACTOR)
        else: n=round(counts[SPECS[t]['fk'][1]]*SPECS[t]['fk'][2])
        if n<0: raise ValueError('Negative row count: '+t)
        counts[t]=n
    for t in ORDER:
        f=SPECS[t]['fk']
        if f and counts[t]!=counts[f[1]]*f[3]:
            raise ValueError(f'{t}: override violates mandatory {f[3]} children per parent')
    return counts

def capacity_check(counts,win):
    errors=[]
    for t,n in counts.items():
        cap=win['end']-(4-SPECS[t]['depth'])*DAY-win['start']+1
        if cap<1 or n>cap: errors.append(f'{t}.created_d: requested {n:,}, at most {max(0,cap):,} second-resolution slots')
        for c,k in COLS[t]:
            if c not in SPECS[t]['unique']: continue
            if k=='f' and c in SPECS[t]['ranges']:
                dec=2 if c=='offer_amt' else 5; lo,hi=SPECS[t]['ranges'][c]
                m=round(hi*10**dec)-round(lo*10**dec)+1
                if n>m: errors.append(f'{t}.{c}: requested {n:,}, only {m:,} unique values at {dec} decimals')
        if t==A+'as_applicant_detail' and n>SSN_HI-SSN_LO: errors.append('SSN domain exhausted')
        if t==D+'das_member_origination' and n>MEMBER_REF_MAX-MEMBER_REF_MIN+1: errors.append('member_ref_id domain exhausted')
    if errors: raise ValueError('Infeasible without changing business logic:\n'+'\n'.join(errors))

class Bitmap:
    """Packed, disk-backed exact membership: one bit per finite-domain value."""
    def __init__(self,path,lo,hi):
        self.lo,self.hi=int(lo),int(hi); self.size=self.hi-self.lo+1; self.used=0
        if self.size<1: raise ValueError('Invalid finite domain')
        self.bits=np.memmap(path,dtype=np.uint8,mode='w+',shape=((self.size+7)//8,))
        self.bits[:]=0
    def accept(self,values):
        v=np.asarray(values,dtype=np.int64); ix=v-self.lo
        if np.any((ix<0)|(ix>=self.size)): raise ValueError('Value outside finite uniqueness domain')
        mask=((self.bits[ix>>3] >> (ix&7)) & 1)==0
        # Reserve only first occurrences; OR.at handles shared bytes correctly.
        _, first=np.unique(v,return_index=True)
        first_mask=np.zeros(len(v),dtype=bool); first_mask[first]=True; mask &= first_mask
        good=ix[mask]; np.bitwise_or.at(self.bits,good>>3,(1 << (good&7)).astype(np.uint8))
        self.used+=int(mask.sum()); return mask
    def seed(self,values):
        v=np.asarray(values,dtype=np.int64)
        if len(v) and not self.accept(v).all(): raise ValueError('Existing unique column contains duplicates')
    def draw(self,n,draw):
        if n>self.size-self.used: raise ValueError('Unique finite domain exhausted')
        out=np.empty(n,dtype=np.int64); pending=np.arange(n)
        for attempt in range(4096):
            if not len(pending): return out
            values=np.asarray(draw(pending),dtype=np.int64)
            good=self.accept(values); out[pending[good]]=values[good]; pending=pending[~good]
        raise RuntimeError('Constrained unique sampling saturated; refusing to relax precision, bounds or lag rules')
    def close(self):
        self.bits.flush(); self.bits._mmap.close()

class Ledger:
    def __init__(self,path,time_lo,time_hi):
        self.path=Path(path); self.path.mkdir(parents=True,exist_ok=True)
        self.time_lo,self.time_hi=time_lo,time_hi; self.maps={}
        # Random hexadecimal IDs and the ORIGINAL variable-length email algorithm
        # need an exact external registry, not an in-RAM set or probabilistic hash.
        self.sql=sqlite3.connect(str(self.path/'strings.sqlite'))
        self.sql.execute('PRAGMA journal_mode=OFF'); self.sql.execute('PRAGMA synchronous=OFF')
        self.sql.execute('PRAGMA cache_size=-32768'); self.sql.execute('PRAGMA temp_store=FILE')
        self.sql.execute('CREATE TABLE used (ns TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(ns,value)) WITHOUT ROWID')
    def bitmap(self,t,c,lo,hi):
        key=(t,c)
        if key not in self.maps: self.maps[key]=Bitmap(self.path/(t+'.'+c+'.bits'),lo,hi)
        bm=self.maps[key]
        if (bm.lo,bm.hi)!=(int(lo),int(hi)): raise ValueError('Inconsistent domain')
        return bm
    def times(self,t,c,rng,lo,hi):
        lo=np.asarray(lo,dtype=np.int64); hi=np.asarray(hi,dtype=np.int64)
        if np.any(hi<lo): raise ValueError(f'{t}.{c}: impossible timestamp interval')
        bm=self.bitmap(t,c,self.time_lo,self.time_hi)
        return bm.draw(len(lo),lambda ix:lo[ix]+(rng.random(len(ix))*(hi[ix]-lo[ix]+1)).astype(np.int64))
    def floats(self,t,c,rng,n,dec):
        lo,hi=SPECS[t]['ranges'][c]; scale=10**dec
        bm=self.bitmap(t,c,round(lo*scale),round(hi*scale))
        return bm.draw(n,lambda ix:np.rint(rng.uniform(lo,hi,len(ix))*scale).astype(np.int64))/scale
    def ssns(self,t,rng,n):
        bm=self.bitmap(t,'ssn',SSN_LO,SSN_HI-1)
        v=bm.draw(n,lambda ix:rng.integers(SSN_LO,SSN_HI,len(ix)))
        return np.char.add(np.char.add(np.char.add(np.char.mod('%03d',v//1000000),'-'),np.char.mod('%02d',(v//10000)%100)),np.char.add('-',np.char.mod('%04d',v%10000))).astype(object)
    def members(self,t,rng,n):
        bm=self.bitmap(t,'member_ref_id',MEMBER_REF_MIN,MEMBER_REF_MAX)
        return bm.draw(n,lambda ix:np.exp(rng.uniform(np.log(MEMBER_REF_MIN),np.log(MEMBER_REF_MAX),len(ix))).astype(np.int64))
    def string_accept(self,t,c,values):
        vals=np.asarray(values,dtype=object); _,first=np.unique(vals,return_index=True)
        mask=np.zeros(len(vals),dtype=bool); mask[first]=True; ns=t+'.'+c
        # Batched index probes avoid one SELECT per row.
        for start in range(0,len(first),400):
            ix=first[start:start+400]; v=vals[ix].tolist()
            found={r[0] for r in self.sql.execute('SELECT value FROM used WHERE ns=? AND value IN ('+','.join('?' for _ in v)+')',[ns]+v)}
            mask[ix]=[x not in found for x in v]
        self.sql.executemany('INSERT INTO used VALUES (?,?)',((ns,str(x)) for x in vals[mask])); self.sql.commit()
        return mask
    def hex(self,t,c,rng,n,nbytes):
        out=np.empty(n,dtype=object); pending=np.arange(n)
        while len(pending):
            v=random_hex(rng,len(pending),nbytes); ok=self.string_accept(t,c,v)
            out[pending[ok]]=v[ok]; pending=pending[~ok]
        return out
    def emails(self,t,rng,n):
        bases=rng.choice(POOLS['first'],n); nums=rng.integers(0,10**6,n); doms=rng.choice(EMAIL_DOMAINS,n)
        local=np.char.add(np.char.lower(bases.astype(str)),nums.astype(str)).astype(object)
        need=np.array([max(0,16-len(b)-1-len(dm)) for b,dm in zip(local,doms)])
        for _ in range(int(need.max()) if n else 0):
            ix=np.flatnonzero(need>0); local[ix]=np.char.add(local[ix].astype(str),rng.integers(0,10,len(ix)).astype(str)); need[ix]-=1
        out=np.char.add(np.char.add(local.astype(str),'@'),doms).astype(object); pending=np.arange(n)
        while len(pending):
            ok=self.string_accept(t,'email_address',out[pending]); pending=pending[~ok]
            if len(pending):
                local[pending]=np.char.add(local[pending].astype(str),rng.integers(0,10,len(pending)).astype(str))
                out[pending]=np.char.add(np.char.add(local[pending].astype(str),'@'),doms[pending])
                if np.any(np.char.str_len(out[pending].astype(str))>32): raise RuntimeError('Email length domain saturated')
        return out
    def seed_frame(self,t,df):
        for c,k in COLS[t]:
            if c not in SPECS[t]['unique']: continue
            # FKs are inherited, never randomly generated; no ledger is needed.
            if SPECS[t]['fk'] and c==SPECS[t]['fk'][0]: continue
            vals=df[c].dropna()
            if k=='t': self.bitmap(t,c,self.time_lo,self.time_hi).seed(vals.to_numpy().astype('datetime64[s]').astype('int64'))
            elif c=='ssn': self.bitmap(t,c,SSN_LO,SSN_HI-1).seed(vals.str.replace('-','',regex=False).astype('int64'))
            elif c=='member_ref_id': self.bitmap(t,c,MEMBER_REF_MIN,MEMBER_REF_MAX).seed(vals)
            elif k=='f':
                dec=2 if c=='offer_amt' else 5; lo,hi=SPECS[t]['ranges'][c]
                self.bitmap(t,c,round(lo*10**dec),round(hi*10**dec)).seed(np.rint(vals.to_numpy()*10**dec).astype('int64'))
            elif k=='s' and len(vals) and not self.string_accept(t,c,vals.to_numpy()).all(): raise ValueError('Existing duplicate: '+t+'.'+c)
    def close(self):
        for bm in self.maps.values(): bm.close()
        self.sql.close()

def generate(t,n,rng,win,ledger,fks=None,parent_created=None,update=False):
    sp=SPECS[t]; name=t.split('.')[1]; d={}; E=ENUMS.get(t,{})
    hi_c=win['end']-(4-sp['depth'])*DAY
    lo=np.full(n,win['start'],dtype=np.int64) if parent_created is None else np.maximum(parent_created,win['start'])
    hi=np.full(n,hi_c,dtype=np.int64) if parent_created is None else np.minimum(lo+CHILD_LAG_DAYS*DAY,hi_c)
    # These candidate created_d values are discarded on update, as in the original.
    d['created_d']=ledger.times(t,'candidate_created_d' if update else 'created_d',rng,lo,hi)
    d['modified_d']=ledger.times(t,'modified_d',rng,d['created_d']+1,np.minimum(d['created_d']+MOD_LAG_DAYS*DAY,win['end']))
    d['id']=ledger.hex(t,'candidate_id' if update else 'id',rng,n,sp['idb'])
    if sp['fk']: d[sp['fk'][0]]=fks
    def event(c,base,offset,lag): d[c]=ledger.times(t,c,rng,base+1,np.minimum(base+lag*DAY,win['end']-offset*DAY))
    for c,k in COLS[t]:
        if c in d or k=='t': continue
        if name=='das_origination_ext_ref' and c in ('ext_src_id','ext_entity_name'):
            rank=pd.Series(fks).groupby(fks,sort=False).cumcount().to_numpy()%2
            d[c]=np.array([11000,11004] if c=='ext_src_id' else ['prescreenentry','applicationid'])[rank]
        elif c in E: d[c]=pick(rng,n,E[c])
        elif c in sp['nulls'] and sp['nulls'][c]==1.: d[c]=np.full(n,np.nan if k=='f' else None)
        elif c in sp['ranges']:
            a,b=sp['ranges'][c]
            if c=='member_ref_id': d[c]=ledger.members(t,rng,n)
            elif k=='i': d[c]=rng.integers(int(a),int(b)+1,n)
            elif c=='apy': d[c]=rng.integers(round(a*100),round(b*100)+1,n)/100.
            elif c in sp['unique']: d[c]=ledger.floats(t,c,rng,n,2 if c=='offer_amt' else 5)
            else: d[c]=rng.uniform(a,b,n)
        elif c in ('first_name','last_name'): d[c]=rng.choice(POOLS['first' if c=='first_name' else 'last'],n)
        elif c=='date_of_birth': d[c]=rng.integers(ts(DOB_MIN)//DAY,ts(DOB_MAX)//DAY+1,n)*DAY
        elif c=='ssn': d[c]=ledger.ssns(t,rng,n)
        elif c=='email_address': d[c]=ledger.emails(t,rng,n)
        elif c in ('origination_guid','ext_ref_id','subject_ref_id','subject_src_id') or (name=='uw_process_run_ext_ref' and c=='ext_src_id'): d[c]=ledger.hex(t,c,rng,n,16)
        elif c.endswith('_keyn'): d[c]=np.full(n,KEY_EMAIL_FEK if c=='email_address_keyn' else KEY_NAME_FEK,dtype=object)
        elif c=='product_type_cd': d[c]=np.full(n,'PERSONAL_LOAN',dtype=object)
        elif c=='source_cd': d[c]=np.full(n,'APPLICANT',dtype=object)
        elif c=='account_type': d[c]=np.full(n,{'as_application_certificate_of_d':'CD','as_application_checking_account':'ND','as_application_saving_account':'SV'}[name],dtype=object)
        else: raise KeyError('Unhandled column '+t+'.'+c)
        if c in sp['nulls'] and sp['nulls'][c]<1.:
            v=np.asarray(d[c],dtype=object); v[rng.random(n)<sp['nulls'][c]]=None
            d[c]=np.array([np.nan if x is None else x for x in v],dtype=float) if k=='f' else v
    if name=='as_application': event('application_d',d['created_d'],3,1)
    if name=='ps_prescreen_entry':
        event('effective_start_d',d['created_d'],3,1); event('first_look_up_d',d['created_d'],3,EVENT_LAG_DAYS)
        event('last_look_up_d',d['first_look_up_d'],2,EVENT_LAG_DAYS); event('used_d',d['last_look_up_d'],1,EVENT_LAG_DAYS)
    if name=='uw_process':
        event('status_d',d['created_d'],3,EVENT_LAG_DAYS); event('decision_status_d',d['status_d'],2,EVENT_LAG_DAYS)
        event('expiration_d',d['decision_status_d'],1,EVENT_LAG_DAYS)
    for c,k in COLS[t]:
        if k in ('t','d'): d[c]=pd.to_datetime(d[c],unit='s')
        elif k=='i': d[c]=np.asarray(d[c],dtype=np.int64)
        elif k=='f': d[c]=np.asarray(d[c],dtype=np.float64)
    return pd.DataFrame({c:d[c] for c,k in COLS[t]})

TYPE_SQL={'s':'VARCHAR','i':'BIGINT','f':'DOUBLE','t':'TIMESTAMP','d':'DATE'}
def q(s): return '"'+str(s).replace('"','""')+'"'
def lit(s): return "'"+str(s).replace("'","''")+"'"
def db_open(path,spill):
    Path(spill).mkdir(parents=True,exist_ok=True)
    con=duckdb.connect(str(path)); con.execute(f"SET memory_limit={lit(DB_MEMORY)}")
    con.execute(f'SET threads={DB_THREADS}'); con.execute(f'SET temp_directory={lit(str(spill))}')
    con.execute('SET preserve_insertion_order=false'); return con

def rel_name(t): return 't'+str(ORDER.index(t))
def file_path(root,t,fmt=None):
    sch,tab=t.split('.'); return Path(root)/sch/(tab+'.'+(fmt or OUTPUT_FORMAT))
def input_expr(path,t,fmt=None):
    fmt=fmt or OUTPUT_FORMAT
    if fmt=='csv':
        types='{'+','.join(lit(c)+':'+lit(TYPE_SQL[k]) for c,k in COLS[t])+'}'
        return f"read_csv({lit(str(path))},header=true,columns={types},nullstr='',dateformat='%Y-%m-%d',timestampformat='%Y-%m-%d %H:%M:%S',auto_detect=false)"
    cols=','.join(f'CAST({q(c)} AS {TYPE_SQL[k]}) AS {q(c)}' for c,k in COLS[t])
    return f'(SELECT {cols} FROM read_parquet({lit(str(path))}))'

def arrow_schema(t,delta=False):
    schema=pa.schema([(c,pa.int64() if k=='i' else pa.float64() if k=='f' else pa.string()) for c,k in COLS[t]])
    return schema.append(pa.field('_operation',pa.string())) if delta else schema
class Writer:
    def __init__(self,root,t,delta=False):
        self.t=t; self.path=file_path(root,t); self.path.parent.mkdir(parents=True,exist_ok=True)
        self.schema=arrow_schema(t,delta); self.rows=0
        self.sink=pa.OSFile(str(self.path),'wb')
        self.writer=pacsv.CSVWriter(self.sink,self.schema,write_options=pacsv.WriteOptions(include_header=True)) if OUTPUT_FORMAT=='csv' else pq.ParquetWriter(self.sink,self.schema,compression='snappy')
    def write(self,df):
        if not len(df): return
        df=df.copy(deep=False)
        for c,k in COLS[self.t]:
            if k in ('t','d'): df[c]=pd.to_datetime(df[c]).dt.strftime('%Y-%m-%d %H:%M:%S' if k=='t' else '%Y-%m-%d')
        tab=pa.Table.from_pandas(df,schema=self.schema,preserve_index=False)
        self.writer.write_table(tab); self.rows+=len(df)
    def close(self): self.writer.close(); self.sink.close()

def validate_db(con,counts=None,check_lags=False):
    reports={}; errors=[]; warns=[]
    for t in ORDER:
        tb=q(rel_name(t)); sp=SPECS[t]; n=con.execute(f'SELECT count(*) FROM {tb}').fetchone()[0]
        reports[t]={'rows':n}
        if counts is not None and n!=counts[t]: errors.append(f'{t}: count {n} != {counts[t]}')
        terms=[]
        for c,k in COLS[t]:
            if c not in sp['nulls']: terms.append(f'{q(c)} IS NULL')
            if c in sp['ranges']:
                lo,hi=sp['ranges'][c]; terms.append(f'{q(c)} < {lo} OR {q(c)} > {hi}')
        for b,a in [('created_d','modified_d')]+sp['temporal']: terms.append(f'{q(a)} < {q(b)}')
        if terms and con.execute(f'SELECT 1 FROM {tb} WHERE '+ ' OR '.join('('+x+')' for x in terms)+' LIMIT 1').fetchone(): errors.append(t+': required/range/temporal violation')
        for c in sp['unique']:
            # One external aggregation at a time bounds peak hash-table memory.
            if con.execute(f'SELECT {q(c)} FROM {tb} GROUP BY {q(c)} HAVING count(*)>1 LIMIT 1').fetchone(): errors.append(t+'.'+c+': duplicate')
        if con.execute(f"SELECT 1 FROM {tb} WHERE length(id)!={sp['idb']*2} OR NOT regexp_full_match(id,'[0-9a-f]+') LIMIT 1").fetchone(): errors.append(t+': ID format')
        if t==A+'as_applicant_detail':
            cond="length(first_name) NOT BETWEEN 3 AND 11 OR length(last_name) NOT BETWEEN 2 AND 11 OR date_of_birth NOT BETWEEN DATE '"+DOB_MIN+"' AND DATE '"+DOB_MAX+"' OR NOT regexp_full_match(ssn,'[1-8][0-9]{2}-[0-9]{2}-[0-9]{4}')"
            if con.execute(f'SELECT 1 FROM {tb} WHERE {cond} LIMIT 1').fetchone(): errors.append(t+': name/DOB/SSN format')
        if t==A+'as_email_address' and con.execute(f"SELECT 1 FROM {tb} WHERE length(email_address) NOT BETWEEN 16 AND 32 OR NOT regexp_full_match(email_address,'[a-z]+[0-9]+@[a-z]+[.][a-z]+') LIMIT 1").fetchone(): errors.append(t+': email format')
        for c,k in COLS[t]:
            if k=='f' and c in sp['unique']:
                dec=2 if c=='offer_amt' else 5
                if con.execute(f'SELECT 1 FROM {tb} WHERE abs({q(c)}-round({q(c)},{dec}))>1e-8 LIMIT 1').fetchone(): errors.append(t+'.'+c+': precision')
        for c,p in sp['nulls'].items():
            nulls=con.execute(f'SELECT count(*) FILTER (WHERE {q(c)} IS NULL) FROM {tb}').fetchone()[0]
            if p==1. and nulls!=n: errors.append(t+'.'+c+': must be all null')
            if n>=500 and abs(nulls/n-p)>.05: warns.append(t+'.'+c+': null-frequency deviation')
        for c,freq in ENUMS.get(t,{}).items():
            obs=dict(con.execute(f'SELECT {q(c)},count(*) FROM {tb} WHERE {q(c)} IS NOT NULL GROUP BY {q(c)}').fetchall())
            if set(obs)-set(freq): errors.append(t+'.'+c+': unexpected enum')
            total=sum(obs.values()); den=sum(freq.values())
            if n>=500 and total and max(abs(obs.get(k,0)/total-v/den) for k,v in freq.items())>DIST_TOLERANCE: warns.append(t+'.'+c+': enum-frequency deviation')
        f=sp['fk']
        if f:
            col,parent,avg,mn,mx=f; pt=q(rel_name(parent))
            if con.execute(f'SELECT 1 FROM {tb} c ANTI JOIN {pt} p ON c.{q(col)}=p.id LIMIT 1').fetchone(): errors.append(t+': orphan FK')
            if con.execute(f'SELECT 1 FROM {pt} p LEFT JOIN (SELECT {q(col)} AS fk,count(*) AS n FROM {tb} GROUP BY {q(col)}) c ON p.id=c.fk WHERE coalesce(n,0)<{mn} OR coalesce(n,0)>{mx} LIMIT 1').fetchone(): errors.append(t+': cardinality')
            if check_lags and con.execute(f'SELECT 1 FROM {tb} c JOIN {pt} p ON c.{q(col)}=p.id WHERE c.created_d < p.created_d OR c.created_d > p.created_d + INTERVAL {CHILD_LAG_DAYS} DAY LIMIT 1').fetchone(): errors.append(t+': parent lag')
        if t==D+'das_origination_ext_ref':
            if con.execute(f"SELECT 1 FROM {tb} WHERE NOT ((ext_src_id=11000 AND ext_entity_name='prescreenentry') OR (ext_src_id=11004 AND ext_entity_name='applicationid')) LIMIT 1").fetchone(): errors.append(t+': source/entity pairing')
            if con.execute(f'SELECT origination_id FROM {tb} GROUP BY origination_id HAVING count(*)!=2 OR count(DISTINCT ext_src_id)!=2 LIMIT 1').fetchone(): errors.append(t+': missing source/entity pair')
        reports[t]['validated']=True
    for w in warns: print('WARN:',w)
    if errors: raise AssertionError('\n'.join(errors))
    print('Exact global validation PASSED:',sum(x['rows'] for x in reports.values()),'rows in',len(reports),'tables')
    return {'tables':reports,'warnings':warns,'passed':True}

def family_tables(root):
    family={root}
    for t in ORDER:
        if SPECS[t]['fk'] and SPECS[t]['fk'][1] in family: family.add(t)
    return [t for t in ORDER if t in family]

def full_family(root,counts,stage,work,index):
    init_pools(SEED); rng=np.random.default_rng(np.random.SeedSequence([SEED,index,0]))
    win={'start':ts(START_DATE),'end':ts(END_DATE)}; tables=family_tables(root)
    ledger=Ledger(Path(work)/('family'+str(index)),win['start'],win['end'])
    writers={t:Writer(stage,t) for t in tables}
    try:
        # Child bundles travel together, so no entire-table parent map is retained.
        for offset in range(0,counts[root],BATCH_ROWS):
            n=min(BATCH_ROWS,counts[root]-offset); refs={}
            for t in tables:
                f=SPECS[t]['fk']
                if not f: df=generate(t,n,rng,win,ledger)
                else:
                    col,parent,avg,mn,mx=f; ids,created=refs[parent]
                    ix=np.repeat(np.arange(len(ids)),mn); rng.shuffle(ix)
                    df=generate(t,len(ix),rng,win,ledger,ids[ix],created[ix])
                writers[t].write(df)
                if any(SPECS[x]['fk'] and SPECS[x]['fk'][1]==t for x in tables):
                    refs[t]=(df['id'].to_numpy(copy=True),df['created_d'].to_numpy().astype('datetime64[s]').astype('int64'))
            del refs
        return {t:w.rows for t,w in writers.items()}
    finally:
        for w in writers.values(): w.close()
        ledger.close(); shutil.rmtree(ledger.path,ignore_errors=True)

def s3_client():
    import boto3
    return boto3.client('s3')
def publish(stage,prefix,report):
    # The original schema/table filename convention is retained (one CSV per table).
    paths=[]
    for t in ORDER:
        src=file_path(stage,t); rel=str(src.relative_to(stage)); key=prefix.rstrip('/')+'/'+rel
        if STORAGE=='local':
            dest=Path(LOCAL_DIR)/key; dest.parent.mkdir(parents=True,exist_ok=True)
            tmp=dest.with_name(dest.name+'.tmp-'+uuid.uuid4().hex); shutil.copyfile(src,tmp); os.replace(tmp,dest)
            paths.append(str(dest))
        else:
            from boto3.s3.transfer import TransferConfig
            s3_client().upload_file(str(src),S3_BUCKET,key,Config=TransferConfig(multipart_threshold=16*1024**2,multipart_chunksize=16*1024**2,max_concurrency=min(4,DB_THREADS),use_threads=True))
            paths.append('s3://'+S3_BUCKET+'/'+key)
    manifest={'seed':SEED,'mode':MODE,'format':OUTPUT_FORMAT,'batch_rows':BATCH_ROWS,'workers':WORKERS,'validation':report,'files':paths}
    data=json.dumps(manifest,indent=2).encode(); key=prefix.rstrip('/')+'/manifest.json'
    if STORAGE=='local':
        dest=Path(LOCAL_DIR)/key; tmp=dest.with_suffix('.json.tmp'); tmp.write_bytes(data); os.replace(tmp,dest)
    else: s3_client().put_object(Bucket=S3_BUCKET,Key=key,Body=data)
    return paths

def run_full():
    counts=target_counts(); win={'start':ts(START_DATE),'end':ts(END_DATE)}; capacity_check(counts,win)
    Path(SCRATCH_DIR).mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='full-',dir=SCRATCH_DIR) as work:
        stage=Path(work)/'output'; stage.mkdir(); roots=[t for t in ORDER if not SPECS[t]['fk']]
        if WORKERS>1:
            context=mp.get_context('fork' if 'fork' in mp.get_all_start_methods() else 'spawn')
            with ProcessPoolExecutor(max_workers=min(WORKERS,len(roots)),mp_context=context) as executor:
                jobs=[executor.submit(full_family,r,counts,str(stage),work,i) for i,r in enumerate(roots)]
                for job in jobs: job.result()
        else:
            for i,r in enumerate(roots): full_family(r,counts,str(stage),work,i)
        con=db_open(':memory:',Path(work)/'spill')
        try:
            for t in ORDER: con.execute(f'CREATE VIEW {q(rel_name(t))} AS SELECT * FROM {input_expr(file_path(stage,t),t)}')
            report=validate_db(con,counts,check_lags=True)
        finally: con.close()
        files=publish(stage,S3_PREFIX,report)
    return report,files

# Incremental operations are materialized in a spilling on-disk database;
# generation, updates and output are processed in at most 2 * BATCH_ROWS each (the sole two-child relation).
def create_empty(con,name,t):
    con.execute(f'CREATE TABLE {q(name)} ('+','.join(q(c)+' '+TYPE_SQL[k] for c,k in COLS[t])+')')
def append_frame(con,name,df):
    if not len(df): return
    con.register('_batch',df)
    try: con.execute(f'INSERT INTO {q(name)} SELECT * FROM _batch')
    finally: con.unregister('_batch')
def batches(con,sql):
    # A separate cursor prevents INSERT/UPDATE invalidating the streaming SELECT.
    cur=con.cursor(); reader=None
    try:
        reader=cur.execute(sql).fetch_record_batch(BATCH_ROWS)
        for batch in reader: yield batch.to_pandas()
    finally:
        if reader is not None and hasattr(reader,'close'): reader.close()
        cur.close()
def mutable_cols(t):
    sp=SPECS[t]; skip={'id','created_d','modified_d','version'}|set(sp['nonmut'])
    if sp['fk']: skip.add(sp['fk'][0])
    return [c for c,k in COLS[t] if c not in skip]
def load_disk(con,work):
    counts={}
    for t in ORDER:
        key=S3_PREFIX.rstrip('/')+'/'+t.split('.')[0]+'/'+t.split('.')[1]+'.'+OUTPUT_FORMAT
        if STORAGE=='local': src=Path(LOCAL_DIR)/key
        else:
            src=Path(work)/('input.'+OUTPUT_FORMAT); s3_client().download_file(S3_BUCKET,key,str(src))
        con.execute(f'CREATE TABLE {q(rel_name(t))} AS SELECT * FROM {input_expr(src,t)}')
        counts[t]=con.execute(f'SELECT count(*) FROM {q(rel_name(t))}').fetchone()[0]
        if STORAGE=='s3': src.unlink()
    return counts

def run_incremental():
    init_pools(SEED); Path(SCRATCH_DIR).mkdir(parents=True,exist_ok=True)
    rng=np.random.default_rng(np.random.SeedSequence([SEED,1]))
    with tempfile.TemporaryDirectory(prefix='incremental-',dir=SCRATCH_DIR) as work:
        con=db_open(Path(work)/'data.duckdb',Path(work)/'spill'); stage=Path(work)/'output'; stage.mkdir()
        try:
            base_counts=load_disk(con,work)
            # Validate inputs before performing cascades or sampling.
            validate_db(con)
            maxima=[]; minima=[]
            for t in ORDER:
                cols=[c for c,k in COLS[t] if k=='t']
                row=con.execute('SELECT '+','.join('min('+q(c)+'),max('+q(c)+')' for c in cols)+' FROM '+q(rel_name(t))).fetchone()
                for i in range(0,len(row),2):
                    if row[i] is not None: minima.append(ts(row[i])); maxima.append(ts(row[i+1]))
            if not maxima: max_ts=ts(END_DATE); time_lo=ts(START_DATE)
            else: max_ts=max(maxima); time_lo=min(minima)
            start=(max_ts//DAY+1)*DAY; half=(INCR_WINDOW_DAYS//2)*DAY
            if half<5*DAY: raise ValueError('Incremental half-window must reserve at least five days for chained events')
            upd={'start':start,'end':start+half-1}; ins={'start':start+half,'end':start+2*half}
            stats={}; counts_insert={}
            for t in ORDER:
                if t in GOLDEN_TABLES: counts_insert[t]=0
                elif SPECS[t]['fk']: counts_insert[t]=counts_insert[SPECS[t]['fk'][1]]*SPECS[t]['fk'][3]
                else: counts_insert[t]=round(base_counts[t]*INSERT_PCT/100.)
            capacity_check(counts_insert,ins)
            # Choose roots exactly once; descendants derive deletion IDs via joins.
            for ti,t in enumerate(ORDER):
                tb=rel_name(t); f=SPECS[t]['fk']; k=0 if t in GOLDEN_TABLES else round(base_counts[t]*DELETE_PCT/100.)
                if t in GOLDEN_TABLES: query=f'SELECT id FROM {q(tb)} WHERE false'
                elif not f:
                    query=f'SELECT id FROM {q(tb)} USING SAMPLE {k} ROWS (reservoir, {SEED+ti})' if k else f'SELECT id FROM {q(tb)} WHERE false'
                else: query=f'SELECT c.id FROM {q(tb)} c JOIN {q("delid"+str(ORDER.index(f[1])))} p ON c.{q(f[0])}=p.id'
                con.execute(f'CREATE TABLE {q("delid"+str(ti))} AS {query}')
            for ti in reversed(range(len(ORDER))):
                t=ORDER[ti]; tb=rel_name(t); ids=q('delid'+str(ti))
                con.execute(f'CREATE TABLE {q("deleted"+str(ti))} AS SELECT b.* FROM {q(tb)} b JOIN {ids} d ON b.id=d.id')
                con.execute(f'DELETE FROM {q(tb)} WHERE id IN (SELECT id FROM {ids})')
            for ti,t in enumerate(ORDER):
                tb=rel_name(t); sp=SPECS[t]; nt=con.execute(f'SELECT count(*) FROM {q(tb)}').fetchone()[0]
                k=0 if t in GOLDEN_TABLES else round(nt*UPDATE_PCT/100.)
                if k>upd['end']-upd['start']-(4-sp['depth'])*DAY+1: raise ValueError(t+': update count exceeds timestamp capacity')
                ledger=Ledger(Path(work)/('ledger'+str(ti)),time_lo,ins['end'])
                try:
                    # Seed from survivors, so unchanged values cannot collide with updates/inserts.
                    for df in batches(con,f'SELECT * FROM {q(tb)}'): ledger.seed_frame(t,df)
                    selection=f'SELECT * FROM {q(tb)} USING SAMPLE {k} ROWS (reservoir, {SEED+100+ti})' if k else f'SELECT * FROM {q(tb)} WHERE false'
                    con.execute(f'CREATE TABLE {q("selected"+str(ti))} AS {selection}')
                    create_empty(con,'updated'+str(ti),t)
                    for old in batches(con,f'SELECT * FROM {q("selected"+str(ti))}'):
                        f=sp['fk']; new=generate(t,len(old),rng,upd,ledger,old[f[0]].to_numpy() if f else None,update=True)
                        # Conservatively reserve old unique values even for updated rows.
                        # This retains original nonmutable fields and PK/FK values.
                        out=old.copy()
                        for c in mutable_cols(t): out[c]=new[c].to_numpy()
                        out['modified_d']=new['modified_d'].to_numpy()
                        out['version']=np.minimum(old['version'].to_numpy()+1,sp['ranges']['version'][1])
                        append_frame(con,'updated'+str(ti),out)
                    assignments=','.join(f'{q(c)}=u.{q(c)}' for c in mutable_cols(t)+['modified_d','version'])
                    con.execute(f'UPDATE {q(tb)} b SET {assignments} FROM {q("updated"+str(ti))} u WHERE b.id=u.id')
                    create_empty(con,'inserted'+str(ti),t)
                    f=sp['fk']
                    if not f:
                        for off in range(0,counts_insert[t],BATCH_ROWS):
                            new=generate(t,min(BATCH_ROWS,counts_insert[t]-off),rng,ins,ledger); append_frame(con,'inserted'+str(ti),new)
                    else:
                        col,parent,avg,mn,mx=f; pn='inserted'+str(ORDER.index(parent))
                        for pdf in batches(con,f'SELECT id,created_d FROM {q(pn)}'):
                            ix=np.repeat(np.arange(len(pdf)),mn); rng.shuffle(ix)
                            new=generate(t,len(ix),rng,ins,ledger,pdf.id.to_numpy()[ix],pdf.created_d.to_numpy().astype('datetime64[s]').astype('int64')[ix])
                            append_frame(con,'inserted'+str(ti),new)
                    con.execute(f'INSERT INTO {q(tb)} SELECT * FROM {q("inserted"+str(ti))}')
                    stats[t]={op:con.execute(f'SELECT count(*) FROM {q(tab+str(ti))}').fetchone()[0] for op,tab in [('DELETE','deleted'),('UPDATE','updated'),('INSERT','inserted')]}
                finally:
                    ledger.close(); shutil.rmtree(ledger.path,ignore_errors=True)
            expected={t:base_counts[t]-stats[t]['DELETE']+stats[t]['INSERT'] for t in ORDER}
            report=validate_db(con,expected,check_lags=True); report['operations']=stats
            for ti,t in enumerate(ORDER):
                delta=INCREMENTAL_OUTPUT=='delta'; writer=Writer(stage,t,delta)
                try:
                    if delta:
                        for op,tab in [('DELETE','deleted'),('UPDATE','updated'),('INSERT','inserted')]:
                            for df in batches(con,f'SELECT * FROM {q(tab+str(ti))}'):
                                df['_operation']=op; writer.write(df)
                    else:
                        for df in batches(con,f'SELECT * FROM {q(rel_name(t))}'): writer.write(df)
                finally: writer.close()
            return report,publish(stage,INCREMENTAL_PREFIX,report)
        finally: con.close()

def self_test():
    """Only 1,000 rows/root; exercises parallel/batch writes and both incremental views."""
    global STORAGE,MODE,INCREMENTAL_OUTPUT,SCALE_FACTOR,WORKERS,INCREMENTAL_PREFIX
    STORAGE='local'; MODE='full'; SCALE_FACTOR=1.; WORKERS=min(2,os.cpu_count() or 1)
    ROOT_ROW_COUNTS.update({t:1000 for t in ROOT_ROW_COUNTS})
    full,files=run_full(); print('Full CSV output:',str(Path(LOCAL_DIR)/S3_PREFIX))
    MODE='incremental'; INCREMENTAL_OUTPUT='delta'
    report,delta_files=run_incremental()
    assert all(report['operations'][t]['DELETE']==(40 if t==D+'das_origination_ext_ref' else 20) for t in ORDER)
    assert all(report['operations'][t]['UPDATE']==(98 if t==D+'das_origination_ext_ref' else 49) for t in ORDER)
    assert all(report['operations'][t]['INSERT']==(200 if t==D+'das_origination_ext_ref' else 100) for t in ORDER)
    # Independently replay emitted deltas against the full baseline in a disk DB,
    # compare unchanged fields, and check exact expected operation/merged counts.
    with tempfile.TemporaryDirectory(prefix='replay-',dir=SCRATCH_DIR) as work:
        con=db_open(Path(work)/'replay.duckdb',Path(work)/'spill')
        try:
            for t in ORDER:
                src=file_path(Path(LOCAL_DIR)/S3_PREFIX,t)
                con.execute(f'CREATE TABLE {q(rel_name(t))} AS SELECT * FROM {input_expr(src,t)}')
                df=pd.read_csv(file_path(Path(LOCAL_DIR)/INCREMENTAL_PREFIX,t),dtype={c:'string' for c,k in COLS[t] if k=='s'})
                for c,k in COLS[t]:
                    if k in ('t','d'): df[c]=pd.to_datetime(df[c])
                op=df.pop('_operation'); deleted=df[op=='DELETE']; updated=df[op=='UPDATE']; inserted=df[op=='INSERT']
                assert len(deleted)+len(updated)+len(inserted)==sum(report['operations'][t].values())
                old=con.execute(f'SELECT * FROM {q(rel_name(t))}').fetchdf().set_index('id')
                preserved=['created_d']+SPECS[t]['nonmut']+([SPECS[t]['fk'][0]] if SPECS[t]['fk'] else [])
                for c in preserved:
                    a=updated.set_index('id')[c]; b=old.loc[a.index,c]
                    assert np.array_equal(a.to_numpy(),b.to_numpy()),(t,c)
                con.register('_delta',df.assign(_operation=op))
                con.execute(f"DELETE FROM {q(rel_name(t))} WHERE id IN (SELECT id FROM _delta WHERE _operation IN ('DELETE','UPDATE'))")
                cols=','.join(q(c) for c,k in COLS[t])
                con.execute(f"INSERT INTO {q(rel_name(t))} SELECT {cols} FROM _delta WHERE _operation IN ('UPDATE','INSERT')")
                con.unregister('_delta')
            validate_db(con,{t:(2160 if t==D+'das_origination_ext_ref' else 1080) for t in ORDER},check_lags=True)
        finally: con.close()
    INCREMENTAL_OUTPUT='merged'
    saved_prefix=INCREMENTAL_PREFIX; INCREMENTAL_PREFIX=S3_PREFIX+'incremental_merged/'
    try:
        merged_report,merged_files=run_incremental()
        assert {t:merged_report['tables'][t]['rows'] for t in ORDER} == {t:report['tables'][t]['rows'] for t in ORDER}
        con=db_open(':memory:',Path(SCRATCH_DIR)/'merged-validation-spill')
        try:
            for t in ORDER:
                con.execute(f'CREATE VIEW {q(rel_name(t))} AS SELECT * FROM {input_expr(file_path(Path(LOCAL_DIR)/INCREMENTAL_PREFIX,t),t)}')
            validate_db(con,{t:(2160 if t==D+'das_origination_ext_ref' else 1080) for t in ORDER},check_lags=True)
        finally: con.close()
    finally: INCREMENTAL_PREFIX=saved_prefix
    # Capacity guards are tested mathematically, not by generating large data.
    bad={t:1000 for t in ORDER}; bad[P+'ps_prescreen_entry']=10_000_000
    try: capacity_check(bad,{'start':ts(START_DATE),'end':ts(END_DATE)})
    except ValueError as exc: assert 'offer_amt' in str(exc)
    else: raise AssertionError('Missing finite offer-amount capacity guard')
    print('SELF TEST PASSED: CSV roundtrip, global uniqueness, FK cardinalities, cascades, delta replay, merged roundtrip, immutability, capacity guard')
    return {'full':full,'incremental':report,'files':files,'delta_files':delta_files,'merged_files':merged_files}

def main():
    if STORAGE not in ('local','s3') or MODE not in ('full','incremental') or OUTPUT_FORMAT not in ('csv','parquet') or INCREMENTAL_OUTPUT not in ('merged','delta'): raise ValueError('Invalid configuration')
    if BATCH_ROWS<1 or WORKERS<1: raise ValueError('Batch/workers must be positive')
    if not (0<=UPDATE_PCT<=100 and 0<=DELETE_PCT<=100 and INSERT_PCT>=0): raise ValueError('Invalid incremental percentages')
    if SELF_TEST: result=self_test()
    else:
        report,files=run_full() if MODE=='full' else run_incremental(); result={'report':report,'files':files}
    summary=Path(LOCAL_DIR)/'validation_summary.json'; summary.parent.mkdir(parents=True,exist_ok=True); summary.write_text(json.dumps(result,indent=2))
    print('Validation summary:',summary)
    print('Output format:',OUTPUT_FORMAT,'; batch rows:',BATCH_ROWS)
    return result
if __name__=='__main__': main()
