"""LC20 optimized synthetic generator: bounded batches, exact global uniqueness, CSV.
Dependencies: numpy pandas Faker pyarrow duckdb; boto3 for S3 only.
Run with environment variables (no source editing required):
  LC20_STORAGE=local LC20_LOCAL_DIR=/data/out python lc20_optimized.py
  LC20_SCALE_FACTOR=6250 requests 100M TOTAL rows at the original ratios.
  LC20_BATCH_ROWS=50000 LC20_WORKERS=4 enables parallel independent families.
  LC20_MODE=incremental LC20_INCREMENTAL_OUTPUT=delta (or merged).
  LC20_SCRATCH_DIR=/large/fast/volume LC20_DB_MEMORY=2GB LC20_DB_THREADS=4.
  LC20_SELF_TEST=1 runs only 1,000 rows/root and both incremental modes locally.

Default bucket/prefix, schema, precision, distributions and lag rules are unchanged.
At equal scaling, cent-precision unique offer_amt limits full output to at most
159,690,848 total rows (9,980,678 per root). Other timestamp/lag constraints may
lower practical capacity. 1B at these ratios is impossible without rule changes.
The script fails safely rather than relaxing rules or emitting invalid output.
Incremental default 10-day halves ALSO have finite timestamp capacities.

Generation uses exact disk bitmaps for finite domains, and an indexed disk ledger
for random strings. No approximate uniqueness checks or whole-table Python sets.
CSV is written batch-by-batch, one file per schema/table, with one header. Optional
Parquet output retains the original serialized string dates. Output is staged,
validated in a spilling single-host DuckDB database, then published; manifest last.
Provision scratch for output + string indexes + validation DB + spill. A standard
multicore host is sufficient; large CSV output cannot fit a 10GB scratch volume.
Random data is reproducible for fixed configuration; not bitwise equal to original.
CSV quoting may differ but column order, types and empty-null semantics do not.
"""
import os, json, math, uuid, shutil, sqlite3, tempfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import duckdb
from faker import Faker

SEED=int(os.getenv('LC20_SEED','42'))
SCALE_FACTOR=float(os.getenv('LC20_SCALE_FACTOR','1'))
S3_BUCKET=os.getenv('LC20_S3_BUCKET','mm-fsi-fix')
S3_PREFIX=os.getenv('LC20_S3_PREFIX','lc/synthetic_data_1/runs/20261011-223023/')
INCREMENTAL_PREFIX=os.getenv('LC20_INCREMENTAL_PREFIX',S3_PREFIX+'incremental/')
STORAGE=os.getenv('LC20_STORAGE','s3')
LOCAL_DIR=os.getenv('LC20_LOCAL_DIR','/tmp/synthetic_out')
SCRATCH_DIR=os.getenv('LC20_SCRATCH_DIR',LOCAL_DIR+'/.scratch')
OUTPUT_FORMAT=os.getenv('LC20_OUTPUT_FORMAT','csv')
MODE=os.getenv('LC20_MODE','full')
INCREMENTAL_OUTPUT=os.getenv('LC20_INCREMENTAL_OUTPUT','merged')
INSERT_PCT=float(os.getenv('LC20_INSERT_PCT','10'))
UPDATE_PCT=float(os.getenv('LC20_UPDATE_PCT','5'))
DELETE_PCT=float(os.getenv('LC20_DELETE_PCT','2'))
START_DATE=os.getenv('LC20_START_DATE','2025-01-01')
END_DATE=os.getenv('LC20_END_DATE','2025-11-28 23:59:59')
INCR_WINDOW_DAYS=int(os.getenv('LC20_INCR_WINDOW_DAYS','20'))
BATCH_ROWS=int(os.getenv('LC20_BATCH_ROWS','50000'))
WORKERS=int(os.getenv('LC20_WORKERS','1'))
DB_MEMORY=os.getenv('LC20_DB_MEMORY','1GB')
DB_THREADS=int(os.getenv('LC20_DB_THREADS','2'))
SELF_TEST=os.getenv('LC20_SELF_TEST','0')=='1'
DAY=86400
CHILD_LAG_DAYS,MOD_LAG_DAYS,EVENT_LAG_DAYS=7,60,30
FIRST_NAME_POOL,LAST_NAME_POOL=400,550
DOB_MIN,DOB_MAX='1934-12-07','2007-11-17'
MEMBER_REF_MIN,MEMBER_REF_MAX=10836,997603725
SSN_LO,SSN_HI=100_000_000,899_999_999
EMAIL_DOMAINS=['gmail.com','yahoo.com','outlook.com','hotmail.com','aol.com','icloud.com','example.com']
KEY_NAME_FEK='application-store_first_name_fek'
KEY_EMAIL_FEK='application-store_email_address_fek'
DIST_TOLERANCE=.06
A,D,P,U='lc20_as.','lc20_das.','lc20_ps.','lc20_uw_processing.'
ORDER=[A+x for x in ['as_application','as_applicant','as_applicant_detail','as_application_certificate_of_d','as_application_checking_account','as_application_saving_account','as_email_address']]+[D+x for x in ['das_origination','das_member_origination','das_origination_ext_ref']]+[P+x for x in ['ps_prescreen_entry','ps_prescreen_ext_ref']]+[U+x for x in ['uw_process','uw_process_run','uw_process_run_ext_ref']]
ROOT_ROW_COUNTS={A+'as_application':1000,D+'das_origination':1000,P+'ps_prescreen_entry':1000,U+'uw_process':1000}
GOLDEN_TABLES=json.loads(os.getenv('LC20_GOLDEN_TABLES','{}'))
ROW_COUNT_OVERRIDES=json.loads(os.getenv('LC20_ROW_COUNT_OVERRIDES','{}'))
def spec(cols,idb=16,depth=0,fk=None,nonmut=(),unique=(),ranges=None,nulls=None,temporal=()):
    return dict(cols='id:s created_d:t modified_d:t version:i '+cols,idb=idb,depth=depth,fk=fk,nonmut=list(nonmut),unique=['id','created_d','modified_d']+list(unique),ranges=ranges or {},nulls=nulls or {},temporal=list(temporal))
def fk(col,parent,n=1): return (col,parent,float(n),n,n)
SPECS={
A+'as_application':spec('application_d:t product_type_cd:s application_type_cd:s',nonmut=['application_d'],unique=['application_d'],ranges={'version':(0,1)},nulls={'product_type_cd':.904,'application_type_cd':.096}),
A+'as_applicant':spec('application_id:s primary_applicant_flg:i sms_marketing_opt_in_flg:i marketing_attribution_cd:f',depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(0,1),'primary_applicant_flg':(1,1),'sms_marketing_opt_in_flg':(0,1)},nulls={'marketing_attribution_cd':1.}),
A+'as_applicant_detail':spec('applicant_id:s source_cd:s first_name:s first_name_keyn:s first_name_keyv:i last_name:s last_name_keyn:s last_name_keyv:i honorific_cd:f date_of_birth:d date_of_birth_keyn:s date_of_birth_keyv:i ssn:s ssn_keyn:s ssn_keyv:i marital_status_cd:f educational_qualification_cd:f employment_status_cd:s cumulative_job_tenure_months:i annual_income:f annual_additional_income:f annual_income_taxable:f annual_income_non_taxable:f housing_status_cd:s annual_housing_expense:f annual_additional_debt:f fico_range_cd:f fico_min:i fico_max:i citizenship:f',depth=2,fk=fk('applicant_id',A+'as_applicant'),nonmut=['ssn'],unique=['applicant_id','ssn','annual_income','annual_additional_income','annual_income_taxable','annual_income_non_taxable','annual_housing_expense','annual_additional_debt'],ranges={'version':(0,10),'first_name_keyv':(1,1),'last_name_keyv':(1,1),'date_of_birth_keyv':(1,1),'ssn_keyv':(1,1),'cumulative_job_tenure_months':(1,20),'annual_income':(13.5021,9998.64691),'annual_additional_income':(6.20378,9947.23003),'annual_income_taxable':(1.21818,9997.39902),'annual_income_non_taxable':(7.48881,9994.96678),'annual_housing_expense':(23.61788,9996.5062),'annual_additional_debt':(.55862,9982.90525),'fico_min':(1,5),'fico_max':(1,5)},nulls={'honorific_cd':1.,'marital_status_cd':1.,'educational_qualification_cd':1.,'employment_status_cd':.102,'housing_status_cd':.106,'fico_range_cd':1.,'citizenship':1.}),
A+'as_application_certificate_of_d':spec('source_cd:s initial_deposit_amount:i product_code:i application_id:s apy:f term_in_months:i is_joint_flg:i account_type:s',idb=9,depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(0,5),'initial_deposit_amount':(155,99836),'product_code':(6,28),'apy':(3.4,5.),'term_in_months':(6,60),'is_joint_flg':(0,1)}),
A+'as_application_checking_account':spec('application_id:s source_cd:s debit_card_requested_flg:i min_initial_deposit_amount:i initial_deposit_amount:i product_code:i is_joint_flg:i account_type:s',idb=9,depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(0,10),'debit_card_requested_flg':(0,1),'min_initial_deposit_amount':(1,999),'initial_deposit_amount':(3,998),'product_code':(1,53),'is_joint_flg':(0,1)}),
A+'as_application_saving_account':spec('source_cd:s application_id:s initial_deposit_amount:i product_code:i is_joint_flg:i debit_card_requested_flg:i account_type:s',idb=9,depth=1,fk=fk('application_id',A+'as_application'),unique=['application_id'],ranges={'version':(1,10),'initial_deposit_amount':(19,9989),'product_code':(9,25),'is_joint_flg':(0,1),'debit_card_requested_flg':(0,1)}),
A+'as_email_address':spec('applicant_id:s source_cd:s email_address:s email_address_keyn:s email_address_keyv:i',depth=2,fk=fk('applicant_id',A+'as_applicant'),nonmut=['email_address'],unique=['applicant_id','email_address'],ranges={'version':(0,20),'email_address_keyv':(1,1)}),
D+'das_origination':spec('origination_guid:s',nonmut=['origination_guid'],unique=['origination_guid'],ranges={'version':(0,0)}),
D+'das_member_origination':spec('origination_id:s member_ref_id:i member_src_id:i member_entity_name:s',depth=1,fk=fk('origination_id',D+'das_origination'),nonmut=['member_ref_id'],unique=['origination_id','member_ref_id'],ranges={'version':(0,0),'member_ref_id':(MEMBER_REF_MIN,MEMBER_REF_MAX),'member_src_id':(1000,1200)}),
D+'das_origination_ext_ref':spec('origination_id:s ext_ref_id:s ext_src_id:i ext_entity_name:s flow_id:s flow_version:f',depth=1,fk=fk('origination_id',D+'das_origination',2),nonmut=['ext_ref_id','ext_src_id','ext_entity_name'],unique=['ext_ref_id'],ranges={'version':(0,5),'ext_src_id':(11000,11004),'flow_version':(0,5)},nulls={'flow_id':.108,'flow_version':.519}),
P+'ps_prescreen_entry':spec('source_cd:s credit_policy_name:s credit_policy_version:i effective_start_d:t effective_end_d:f status_cd:f first_look_up_d:t last_look_up_d:t used_d:t offer_amt:f offer_purpose_cd:s',unique=['effective_start_d','first_look_up_d','last_look_up_d','used_d','offer_amt'],ranges={'version':(1,862),'credit_policy_version':(100,344),'offer_amt':(70.05,99876.82)},nulls={'source_cd':.42,'credit_policy_name':.581,'effective_end_d':1.,'status_cd':1.,'offer_purpose_cd':.406},temporal=[('first_look_up_d','last_look_up_d'),('last_look_up_d','used_d')]),
P+'ps_prescreen_ext_ref':spec('ps_prescreen_entry_id:s ext_src_id:s ext_ref_id:s ext_entity_name:s ext_ref_direction_cd:s',depth=1,fk=fk('ps_prescreen_entry_id',P+'ps_prescreen_entry'),nonmut=['ext_ref_id'],unique=['ps_prescreen_entry_id','ext_ref_id'],ranges={'version':(0,0)},nulls={'ext_ref_direction_cd':.101}),
U+'uw_process':spec('subject_ref_id:s subject_entity_name:s type_cd:s subject_src_id:s status_cd:s product_type_cd:f status_d:t decision_status_cd:s decision_status_d:t decision_phase_cd:s subject_type_cd:s expiration_d:t',nonmut=['subject_ref_id','subject_src_id'],unique=['subject_ref_id','subject_src_id','status_d','decision_status_d','expiration_d'],ranges={'version':(1,10)},nulls={'type_cd':.403,'product_type_cd':1.,'decision_phase_cd':.097},temporal=[('status_d','decision_status_d'),('decision_status_d','expiration_d')]),
U+'uw_process_run':spec('uw_process_id:s decision_phase_cd:s decision_status_cd:s',depth=1,fk=fk('uw_process_id',U+'uw_process'),unique=['uw_process_id'],ranges={'version':(1,10)}),
U+'uw_process_run_ext_ref':spec('uw_process_run_id:s ext_ref_id:s ext_src_id:s ext_ref_entity_name:s',depth=2,fk=fk('uw_process_run_id',U+'uw_process_run'),nonmut=['ext_ref_id','ext_src_id'],unique=['uw_process_run_id','ext_ref_id','ext_src_id'],ranges={'version':(1,10)})}
SPECS[D+'das_member_origination']['unique'].remove('created_d')
COLS={t:[tuple(x.split(':')) for x in s['cols'].split()] for t,s in SPECS.items()}
ENUMS={
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
U+'uw_process_run_ext_ref':{'version':{1:.129,4:.11,7:.107,3:.106,9:.101,8:.1,2:.094,10:.092,5:.081,6:.08},'ext_ref_entity_name':{'journeyapplicationtoken':.122,'loginactorid':.119,'narmiuuid':.116,'offerid':.114,'entitytoken':.113,'bankaccount':.112,'applicantid':.103,'prescreenentry':.102,'cif':.099}}}
POOLS={}
def ts(s): return int(pd.Timestamp(s).timestamp())
def init_pools(seed):
    fake=Faker('en_US'); fake.seed_instance(seed)
    def pool(fn,n,lo,hi):
        out={}
        for _ in range(50000):
            v=fn()
            if lo<=len(v)<=hi and v.isalpha(): out[v]=None
            if len(out)==n: break
        if not out: raise ValueError('Empty name pool')
        return np.array(list(out),dtype=object)
    POOLS['first']=pool(fake.first_name,FIRST_NAME_POOL,3,11)
    POOLS['last']=pool(fake.last_name,LAST_NAME_POOL,2,11)
def pick(rng,n,freq):
    keys=np.array(list(freq)); p=np.array(list(freq.values()),dtype=float); p/=p.sum()
    return keys[rng.choice(len(keys),n,p=p)]
def random_hex(rng,n,nbytes):
    raw=rng.integers(0,256,(n,nbytes),dtype=np.uint8)
    return np.char.decode(np.frombuffer(raw.tobytes().hex().encode(),dtype=f'S{2*nbytes}'),'ascii').astype(object)
def target_counts():
    counts={}
    for t in ORDER:
        n=GOLDEN_TABLES.get(t,ROW_COUNT_OVERRIDES.get(t,None))
        if n is None: n=round(ROOT_ROW_COUNTS[t]*SCALE_FACTOR) if t in ROOT_ROW_COUNTS else round(counts[SPECS[t]['fk'][1]]*SPECS[t]['fk'][2])
        if int(n)!=n or n<0: raise ValueError('Invalid count: '+t)
        counts[t]=int(n)
        f=SPECS[t]['fk']
        if f and counts[t]!=counts[f[1]]*f[3]: raise ValueError(t+': override violates exact child cardinality')
    return counts
def capacity_check(counts,win):
    errors=[]
    for t,n in counts.items():
        cap=win['end']-(4-SPECS[t]['depth'])*DAY-win['start']+1
        if n and n>cap: errors.append(f'{t}: {n:,} rows exceed {max(cap,0):,} created_d slots')
        for c,k in COLS[t]:
            if k=='f' and c in SPECS[t]['unique']:
                lo,hi=SPECS[t]['ranges'][c]; dec=2 if c=='offer_amt' else 5
                cap=round(hi*10**dec)-round(lo*10**dec)+1
                if n>cap: errors.append(f'{t}.{c}: {n:,} exceed {cap:,} unique {dec}-decimal values')
        if t==A+'as_applicant_detail' and n>SSN_HI-SSN_LO: errors.append('SSN domain exhausted')
    if errors: raise ValueError('Infeasible without changing rules:\n'+'\n'.join(errors))

class Bitmap:
    """One bit per finite value, stored in a memory-mapped scratch file."""
    def __init__(self,path,lo,hi):
        self.lo,self.hi=int(lo),int(hi); self.size=self.hi-self.lo+1; self.used=0
        if self.size<=0: raise ValueError('Empty uniqueness domain')
        # New sparse files read as zeros; do NOT touch every page at initialization.
        with open(path,'wb') as f: f.truncate((self.size+7)//8)
        self.bits=np.memmap(path,dtype=np.uint8,mode='r+',shape=((self.size+7)//8,))
    def accept(self,values):
        v=np.asarray(values,dtype=np.int64); ix=v-self.lo
        if np.any((ix<0)|(ix>=self.size)): raise ValueError('Outside uniqueness domain')
        good=((self.bits[ix>>3]>>(ix&7))&1)==0
        _,first=np.unique(v,return_index=True); first_mask=np.zeros(len(v),dtype=bool); first_mask[first]=True; good &= first_mask
        ix=ix[good]; np.bitwise_or.at(self.bits,ix>>3,(1<<(ix&7)).astype(np.uint8))
        self.used+=int(good.sum()); return good
    def release(self,values):
        ix=np.asarray(values,dtype=np.int64)-self.lo
        if len(ix):
            if not np.all((self.bits[ix>>3]>>(ix&7))&1): raise AssertionError('Releasing unused value')
            np.bitwise_and.at(self.bits,ix>>3,(255^(1<<(ix&7))).astype(np.uint8)); self.used-=len(ix)
    def seed(self,values):
        if not self.accept(values).all(): raise ValueError('Existing duplicate unique values')
    def draw(self,n,draw):
        if n>self.size-self.used: raise ValueError('Unique domain exhausted')
        out=np.empty(n,dtype=np.int64); pending=np.arange(n)
        for _ in range(4096):
            if not len(pending): return out
            v=np.asarray(draw(pending),dtype=np.int64); good=self.accept(v)
            out[pending[good]]=v[good]; pending=pending[~good]
        raise RuntimeError('Unique timestamp/value sampling saturated; no constraints were relaxed')
    def close(self): self.bits.flush(); self.bits._mmap.close()

class Ledger:
    def __init__(self,path,time_lo,time_hi):
        self.path=Path(path); self.path.mkdir(parents=True,exist_ok=True)
        self.time_lo,self.time_hi=time_lo,time_hi; self.maps={}
        self.sql=sqlite3.connect(str(self.path/'strings.sqlite'))
        for pragma in ['journal_mode=OFF','synchronous=OFF','cache_size=-32768','temp_store=FILE']: self.sql.execute('PRAGMA '+pragma)
        self.sql.execute('CREATE TABLE used (ns TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(ns,value)) WITHOUT ROWID')
    def bitmap(self,t,c,lo,hi):
        key=(t,c)
        if key not in self.maps: self.maps[key]=Bitmap(self.path/(t+'.'+c+'.bits'),lo,hi)
        bm=self.maps[key]
        if (bm.lo,bm.hi)!=(int(lo),int(hi)): raise ValueError('Domain mismatch')
        return bm
    def times(self,t,c,rng,lo,hi):
        lo=np.asarray(lo,dtype=np.int64); hi=np.asarray(hi,dtype=np.int64)
        if np.any(hi<lo): raise ValueError(t+'.'+c+': impossible lag/window')
        return self.bitmap(t,c,self.time_lo,self.time_hi).draw(len(lo),lambda ix:lo[ix]+(rng.random(len(ix))*(hi[ix]-lo[ix]+1)).astype(np.int64))
    def floats(self,t,c,rng,n,old=None):
        lo,hi=SPECS[t]['ranges'][c]; dec=2 if c=='offer_amt' else 5; scale=10**dec
        bm=self.bitmap(t,c,round(lo*scale),round(hi*scale))
        if old is None: return bm.draw(n,lambda ix:np.rint(rng.uniform(lo,hi,len(ix))*scale).astype(np.int64))/scale
        # Updates keep their old unique value on collision, as in the source script.
        v=np.rint(rng.uniform(lo,hi,n)*scale).astype(np.int64)
        dup=pd.Series(v).duplicated(keep=False).to_numpy(); keep=~dup
        accepted=np.zeros(n,dtype=bool); accepted[keep]=bm.accept(v[keep])
        out=np.rint(old.to_numpy()*scale).astype(np.int64)
        bm.release(out[accepted]); out[accepted]=v[accepted]
        return out/scale
    def ssns(self,t,rng,n):
        v=self.bitmap(t,'ssn',SSN_LO,SSN_HI-1).draw(n,lambda ix:rng.integers(SSN_LO,SSN_HI,len(ix)))
        return np.char.add(np.char.add(np.char.add(np.char.mod('%03d',v//1000000),'-'),np.char.mod('%02d',(v//10000)%100)),np.char.add('-',np.char.mod('%04d',v%10000))).astype(object)
    def members(self,t,rng,n):
        return self.bitmap(t,'member_ref_id',MEMBER_REF_MIN,MEMBER_REF_MAX).draw(n,lambda ix:np.exp(rng.uniform(np.log(MEMBER_REF_MIN),np.log(MEMBER_REF_MAX),len(ix))).astype(np.int64))
    def string_accept(self,t,c,values):
        v=np.asarray(values,dtype=object); _,first=np.unique(v,return_index=True)
        mask=np.zeros(len(v),dtype=bool); mask[first]=True; ns=t+'.'+c
        for off in range(0,len(first),400):
            ix=first[off:off+400]; vals=v[ix].tolist()
            found={row[0] for row in self.sql.execute('SELECT value FROM used WHERE ns=? AND value IN ('+','.join('?' for _ in vals)+')',[ns]+vals)}
            mask[ix]=[x not in found for x in vals]
        # Bounded executemany transaction, never a whole-table Python set.
        self.sql.executemany('INSERT INTO used VALUES (?,?)',((ns,str(x)) for x in v[mask])); self.sql.commit()
        return mask
    def hex(self,t,c,rng,n,nbytes):
        out=np.empty(n,dtype=object); pending=np.arange(n)
        while len(pending):
            v=random_hex(rng,len(pending),nbytes); good=self.string_accept(t,c,v)
            out[pending[good]]=v[good]; pending=pending[~good]
        return out
    def emails(self,t,rng,n):
        bases=rng.choice(POOLS['first'],n); nums=rng.integers(0,10**6,n); doms=rng.choice(EMAIL_DOMAINS,n)
        local=np.char.add(np.char.lower(bases.astype(str)),nums.astype(str)).astype(object)
        need=np.maximum(0,16-np.char.str_len(local.astype(str))-1-np.char.str_len(doms.astype(str)))
        for _ in range(int(need.max()) if n else 0):
            ix=np.flatnonzero(need>0); local[ix]=np.char.add(local[ix].astype(str),rng.integers(0,10,len(ix)).astype(str)); need[ix]-=1
        out=np.char.add(np.char.add(local.astype(str),'@'),doms).astype(object); pending=np.arange(n)
        while len(pending):
            good=self.string_accept(t,'email_address',out[pending]); pending=pending[~good]
            if len(pending):
                local[pending]=np.char.add(local[pending].astype(str),rng.integers(0,10,len(pending)).astype(str))
                out[pending]=np.char.add(np.char.add(local[pending].astype(str),'@'),doms[pending])
                if np.any(np.char.str_len(out[pending].astype(str))>32): raise RuntimeError('Email length exhausted')
        return out
    def seed_frame(self,t,df):
        for c,k in COLS[t]:
            if c not in SPECS[t]['unique'] or (SPECS[t]['fk'] and c==SPECS[t]['fk'][0]): continue
            v=df[c].dropna()
            if k=='t': self.bitmap(t,c,self.time_lo,self.time_hi).seed(v.to_numpy().astype('datetime64[s]').astype('int64'))
            elif c=='ssn': self.bitmap(t,c,SSN_LO,SSN_HI-1).seed(v.str.replace('-','',regex=False).astype('int64'))
            elif c=='member_ref_id': self.bitmap(t,c,MEMBER_REF_MIN,MEMBER_REF_MAX).seed(v)
            elif k=='f':
                lo,hi=SPECS[t]['ranges'][c]; scale=100 if c=='offer_amt' else 100000
                self.bitmap(t,c,round(lo*scale),round(hi*scale)).seed(np.rint(v.to_numpy()*scale).astype(np.int64))
            elif k=='s' and not self.string_accept(t,c,v.to_numpy()).all(): raise ValueError(t+'.'+c+': existing duplicate')
    def close(self):
        for bm in self.maps.values(): bm.close()
        self.sql.close()

def mutable_cols(t):
    sp=SPECS[t]; skip={'id','created_d','modified_d','version'}|set(sp['nonmut'])
    if sp['fk']: skip.add(sp['fk'][0])
    return [c for c,k in COLS[t] if c not in skip]

def generate(t,n,rng,win,ledger,fks=None,parent_created=None,old=None):
    sp=SPECS[t]; name=t.split('.')[1]; E=ENUMS.get(t,{})
    hi_c=win['end']-(4-sp['depth'])*DAY
    lo=np.full(n,win['start'],dtype=np.int64) if parent_created is None else np.maximum(parent_created,win['start'])
    hi=np.full(n,hi_c,dtype=np.int64) if parent_created is None else np.minimum(lo+CHILD_LAG_DAYS*DAY,hi_c)
    update=old is not None; d={} if not update else {c:old[c].to_numpy(copy=True) for c,k in COLS[t]}
    candidate=ledger.times(t,'candidate_created_d' if update else 'created_d',rng,lo,hi)
    if not update:
        d['created_d']=candidate; d['id']=ledger.hex(t,'id',rng,n,sp['idb'])
        if sp['fk']: d[sp['fk'][0]]=fks
    d['modified_d']=ledger.times(t,'modified_d',rng,candidate+1,np.minimum(candidate+MOD_LAG_DAYS*DAY,win['end']))
    wanted=set(mutable_cols(t)) if update else {c for c,k in COLS[t]}-{'id','created_d','modified_d'}
    for c,k in COLS[t]:
        if c not in wanted or (sp['fk'] and c==sp['fk'][0]) or k=='t': continue
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
            elif c in sp['unique']: d[c]=ledger.floats(t,c,rng,n,old[c] if update else None)
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
        else: raise KeyError(t+'.'+c)
        if c in sp['nulls'] and sp['nulls'][c]<1.:
            v=np.array(d[c],dtype=float if k=='f' else object,copy=True)
            v[rng.random(n)<sp['nulls'][c]]=np.nan if k=='f' else None; d[c]=v
    def event(c,base,offset,lag):
        if c in wanted: d[c]=ledger.times(t,c,rng,base+1,np.minimum(base+lag*DAY,win['end']-offset*DAY))
    if name=='as_application': event('application_d',candidate,3,1)
    if name=='ps_prescreen_entry':
        event('effective_start_d',candidate,3,1); event('first_look_up_d',candidate,3,EVENT_LAG_DAYS)
        event('last_look_up_d',d['first_look_up_d'],2,EVENT_LAG_DAYS); event('used_d',d['last_look_up_d'],1,EVENT_LAG_DAYS)
    if name=='uw_process':
        event('status_d',candidate,3,EVENT_LAG_DAYS); event('decision_status_d',d['status_d'],2,EVENT_LAG_DAYS); event('expiration_d',d['decision_status_d'],1,EVENT_LAG_DAYS)
    if update: d['version']=np.minimum(old['version'].to_numpy()+1,sp['ranges']['version'][1])
    for c,k in COLS[t]:
        if k in ('t','d'):
            v=np.asarray(d[c]); d[c]=pd.to_datetime(v,unit='s') if np.issubdtype(v.dtype,np.integer) else pd.to_datetime(v)
        elif k=='i': d[c]=np.asarray(d[c],dtype=np.int64)
        elif k=='f': d[c]=np.asarray(d[c],dtype=np.float64)
    return pd.DataFrame({c:d[c] for c,k in COLS[t]})

TYPE_SQL={'s':'VARCHAR','i':'BIGINT','f':'DOUBLE','t':'TIMESTAMP','d':'DATE'}
def q(s): return '"'+str(s).replace('"','""')+'"'
def lit(s): return "'"+str(s).replace("'","''")+"'"
def tab(t): return 't'+str(ORDER.index(t))
def file_path(root,t):
    sch,name=t.split('.'); return Path(root)/sch/(name+'.'+OUTPUT_FORMAT)
def db_open(path,spill):
    Path(spill).mkdir(parents=True,exist_ok=True)
    con=duckdb.connect(str(path))
    con.execute(f'SET memory_limit={lit(DB_MEMORY)}'); con.execute(f'SET threads={DB_THREADS}')
    con.execute(f'SET temp_directory={lit(str(spill))}'); con.execute('SET preserve_insertion_order=false')
    return con
def input_expr(path,t):
    if OUTPUT_FORMAT=='csv':
        types='{'+','.join(lit(c)+':'+lit(TYPE_SQL[k]) for c,k in COLS[t])+'}'
        return f"read_csv({lit(str(path))},header=true,columns={types},nullstr='',dateformat='%Y-%m-%d',timestampformat='%Y-%m-%d %H:%M:%S',auto_detect=false)"
    cols=','.join(f'CAST({q(c)} AS {TYPE_SQL[k]}) AS {q(c)}' for c,k in COLS[t])
    return f'(SELECT {cols} FROM read_parquet({lit(str(path))}))'
def schema(t,delta=False):
    sc=pa.schema([(c,pa.int64() if k=='i' else pa.float64() if k=='f' else pa.string()) for c,k in COLS[t]])
    return sc.append(pa.field('_operation',pa.string())) if delta else sc
class Writer:
    def __init__(self,root,t,delta=False):
        self.t=t; self.path=file_path(root,t); self.path.parent.mkdir(parents=True,exist_ok=True)
        self.schema=schema(t,delta); self.rows=0; self.sink=pa.OSFile(str(self.path),'wb')
        self.writer=pacsv.CSVWriter(self.sink,self.schema) if OUTPUT_FORMAT=='csv' else pq.ParquetWriter(self.sink,self.schema,compression='snappy')
    def write(self,df):
        if not len(df): return
        df=df.copy(deep=False)
        for c,k in COLS[self.t]:
            if k in ('t','d'): df[c]=pd.to_datetime(df[c]).dt.strftime('%Y-%m-%d %H:%M:%S' if k=='t' else '%Y-%m-%d')
        self.writer.write_table(pa.Table.from_pandas(df,schema=self.schema,preserve_index=False)); self.rows+=len(df)
    def close(self): self.writer.close(); self.sink.close()
def batches(con,sql):
    cur=con.cursor(); reader=None
    try:
        reader=cur.execute(sql).fetch_record_batch(BATCH_ROWS)
        for batch in reader: yield batch.to_pandas()
    finally:
        if reader is not None and hasattr(reader,'close'): reader.close()
        cur.close()
def empty(con,name,t):
    con.execute(f'CREATE TABLE {q(name)} ('+','.join(q(c)+' '+TYPE_SQL[k] for c,k in COLS[t])+')')
def append(con,name,df):
    if not len(df): return
    con.register('_batch',df)
    try: con.execute(f'INSERT INTO {q(name)} SELECT * FROM _batch')
    finally: con.unregister('_batch')
def count(con,name): return con.execute(f'SELECT count(*) FROM {q(name)}').fetchone()[0]

def constant_values(t):
    vals={}
    for c,k in COLS[t]:
        if c.endswith('_keyn'): vals[c]=KEY_EMAIL_FEK if c=='email_address_keyn' else KEY_NAME_FEK
        if c=='source_cd' and t.startswith(A): vals[c]='APPLICANT'
    if t==A+'as_application': vals['product_type_cd']='PERSONAL_LOAN'
    accounts={A+'as_application_certificate_of_d':'CD',A+'as_application_checking_account':'ND',A+'as_application_saving_account':'SV'}
    if t in accounts: vals['account_type']=accounts[t]
    return vals

def validate_db(con,counts=None,full_window=None,check_parent_lags=False):
    errors=[]; warns=[]; reports={}
    for t in ORDER:
        tb=q(tab(t)); sp=SPECS[t]; n=count(con,tab(t)); reports[t]={'rows':n}
        if counts is not None and n!=counts[t]: errors.append(t+': count mismatch')
        terms=[]
        for c,k in COLS[t]:
            if c not in sp['nulls']: terms.append(q(c)+' IS NULL')
            if c in sp['ranges']:
                lo,hi=sp['ranges'][c]; terms.append(f'{q(c)}<{lo} OR {q(c)}>{hi}')
            if k=='f': terms.append(f'{q(c)} IS NOT NULL AND NOT isfinite({q(c)})')
            if k=='t':
                terms.append(f'{q(c)}!=date_trunc(\'second\',{q(c)})')
                if full_window: terms.append(f'{q(c)}<TIMESTAMP {lit(START_DATE)} OR {q(c)}>TIMESTAMP {lit(END_DATE)}')
        for c,v in constant_values(t).items(): terms.append(f'{q(c)}!={lit(v)}')
        for before,after in [('created_d','modified_d')]+sp['temporal']: terms.append(f'{q(after)}<{q(before)}')
        if full_window:
            terms.append(f'modified_d>created_d+INTERVAL {MOD_LAG_DAYS} DAY')
            chains=[]
            if t==A+'as_application': chains=[('created_d','application_d',1)]
            if t==P+'ps_prescreen_entry': chains=[('created_d','effective_start_d',1),('created_d','first_look_up_d',EVENT_LAG_DAYS),('first_look_up_d','last_look_up_d',EVENT_LAG_DAYS),('last_look_up_d','used_d',EVENT_LAG_DAYS)]
            if t==U+'uw_process': chains=[('created_d','status_d',EVENT_LAG_DAYS),('status_d','decision_status_d',EVENT_LAG_DAYS),('decision_status_d','expiration_d',EVENT_LAG_DAYS)]
            for b,a,lag in chains: terms.append(f'{q(a)}<={q(b)} OR {q(a)}>{q(b)}+INTERVAL {lag} DAY')
        if con.execute(f'SELECT 1 FROM {tb} WHERE '+' OR '.join('('+x+')' for x in terms)+' LIMIT 1').fetchone(): errors.append(t+': required/range/constants/time violation')
        for c in sp['unique']:
            if con.execute(f'SELECT {q(c)} FROM {tb} GROUP BY {q(c)} HAVING count(*)>1 LIMIT 1').fetchone(): errors.append(t+'.'+c+': not globally unique')
        if con.execute(f"SELECT 1 FROM {tb} WHERE length(id)!={2*sp['idb']} OR NOT regexp_full_match(id,'[0-9a-f]+') LIMIT 1").fetchone(): errors.append(t+': ID shape')
        for c,k in COLS[t]:
            if c in ('origination_guid','ext_ref_id','subject_ref_id','subject_src_id') or (t==U+'uw_process_run_ext_ref' and c=='ext_src_id'):
                if con.execute(f"SELECT 1 FROM {tb} WHERE NOT regexp_full_match({q(c)},'[0-9a-f]{{32}}') LIMIT 1").fetchone(): errors.append(t+'.'+c+': hex format')
            if k=='f' and c in sp['unique']:
                dec=2 if c=='offer_amt' else 5
                if con.execute(f'SELECT 1 FROM {tb} WHERE abs({q(c)}-round({q(c)},{dec}))>1e-8 LIMIT 1').fetchone(): errors.append(t+'.'+c+': decimal precision')
        if t==A+'as_applicant_detail':
            cond=f"length(first_name) NOT BETWEEN 3 AND 11 OR length(last_name) NOT BETWEEN 2 AND 11 OR date_of_birth NOT BETWEEN DATE {lit(DOB_MIN)} AND DATE {lit(DOB_MAX)} OR NOT regexp_full_match(ssn,'[1-8][0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}')"
            if con.execute(f'SELECT 1 FROM {tb} WHERE {cond} LIMIT 1').fetchone(): errors.append(t+': name/DOB/SSN shape')
        if t==A+'as_email_address' and con.execute(f"SELECT 1 FROM {tb} WHERE length(email_address) NOT BETWEEN 16 AND 32 OR NOT regexp_full_match(email_address,'[a-z]+[0-9]+@[a-z]+[.][a-z]+') LIMIT 1").fetchone(): errors.append(t+': email shape')
        for c,pct in sp['nulls'].items():
            nn=con.execute(f'SELECT count(*) FILTER(WHERE {q(c)} IS NULL) FROM {tb}').fetchone()[0]
            if pct==1. and nn!=n: errors.append(t+'.'+c+': must be all null')
            if n>=500 and abs(nn/n-pct)>.05: warns.append(t+'.'+c+': null rate deviation')
        for c,freq in ENUMS.get(t,{}).items():
            obs=dict(con.execute(f'SELECT {q(c)},count(*) FROM {tb} WHERE {q(c)} IS NOT NULL GROUP BY {q(c)}').fetchall())
            if set(obs)-set(freq): errors.append(t+'.'+c+': invalid enum')
            nn=sum(obs.values()); den=sum(freq.values())
            if n>=500 and nn and max(abs(obs.get(k,0)/nn-v/den) for k,v in freq.items())>DIST_TOLERANCE: warns.append(t+'.'+c+': enum rate deviation')
        f=sp['fk']
        if f:
            col,parent,avg,mn,mx=f; pt=q(tab(parent))
            if con.execute(f'SELECT 1 FROM {tb} c ANTI JOIN {pt} p ON c.{q(col)}=p.id LIMIT 1').fetchone(): errors.append(t+': orphan FK')
            if con.execute(f'SELECT 1 FROM {pt} p LEFT JOIN (SELECT {q(col)} fk,count(*) n FROM {tb} GROUP BY {q(col)}) c ON c.fk=p.id WHERE coalesce(n,0)<{mn} OR coalesce(n,0)>{mx} LIMIT 1').fetchone(): errors.append(t+': child cardinality')
            if check_parent_lags and con.execute(f'SELECT 1 FROM {tb} c JOIN {pt} p ON c.{q(col)}=p.id WHERE c.created_d<p.created_d OR c.created_d>p.created_d+INTERVAL {CHILD_LAG_DAYS} DAY LIMIT 1').fetchone(): errors.append(t+': parent lag')
        if t==D+'das_origination_ext_ref':
            if con.execute(f"SELECT 1 FROM {tb} WHERE NOT ((ext_src_id=11000 AND ext_entity_name='prescreenentry') OR (ext_src_id=11004 AND ext_entity_name='applicationid')) LIMIT 1").fetchone(): errors.append(t+': source/entity pairing')
            if con.execute(f'SELECT origination_id FROM {tb} GROUP BY origination_id HAVING count(*)!=2 OR count(DISTINCT ext_src_id)!=2 LIMIT 1').fetchone(): errors.append(t+': missing DAS pair')
        reports[t]['validated']=True
    for w in warns: print('WARN:',w)
    if errors: raise AssertionError('\n'.join(errors))
    print('GLOBAL VALIDATION PASSED:',sum(x['rows'] for x in reports.values()),'rows;',len(reports),'tables')
    return {'passed':True,'tables':reports,'warnings':warns}

def family_tables(root):
    family={root}
    for t in ORDER:
        if SPECS[t]['fk'] and SPECS[t]['fk'][1] in family: family.add(t)
    return [t for t in ORDER if t in family]
def full_family(root,counts,stage,work,index):
    init_pools(SEED); rng=np.random.default_rng(np.random.SeedSequence([SEED,index,0]))
    win={'start':ts(START_DATE),'end':ts(END_DATE)}; tables=family_tables(root)
    ledger=Ledger(Path(work)/('family'+str(index)),win['start'],win['end']); writers={}
    try:
        for t in tables: writers[t]=Writer(stage,t)
        for offset in range(0,counts[root],BATCH_ROWS):
            n=min(BATCH_ROWS,counts[root]-offset); refs={}
            for t in tables:
                f=SPECS[t]['fk']
                if not f: df=generate(t,n,rng,win,ledger)
                else:
                    ids,created=refs[f[1]]; ix=np.repeat(np.arange(len(ids)),f[3]); rng.shuffle(ix)
                    df=generate(t,len(ix),rng,win,ledger,ids[ix],created[ix])
                writers[t].write(df)
                if any(SPECS[x]['fk'] and SPECS[x]['fk'][1]==t for x in tables): refs[t]=(df.id.to_numpy(copy=True),df.created_d.to_numpy().astype('datetime64[s]').astype('int64'))
            del refs,df
        return {t:w.rows for t,w in writers.items()}
    finally:
        for w in writers.values(): w.close()
        ledger.close(); shutil.rmtree(ledger.path,ignore_errors=True)

def s3_client():
    import boto3
    return boto3.client('s3')
def publish(stage,prefix,report):
    files=[]; prefix=prefix.strip('/')
    client=s3_client() if STORAGE=='s3' else None
    for t in ORDER:
        src=file_path(stage,t); key=prefix+'/'+src.relative_to(stage).as_posix()
        if STORAGE=='local':
            dest=Path(LOCAL_DIR)/key; dest.parent.mkdir(parents=True,exist_ok=True)
            tmp=dest.with_name(dest.name+'.tmp-'+uuid.uuid4().hex)
            shutil.copyfile(src,tmp); os.replace(tmp,dest); files.append(str(dest))
        else:
            from boto3.s3.transfer import TransferConfig
            client.upload_file(str(src),S3_BUCKET,key,Config=TransferConfig(multipart_threshold=16*1024**2,multipart_chunksize=16*1024**2,max_concurrency=min(4,DB_THREADS)))
            files.append('s3://'+S3_BUCKET+'/'+key)
    manifest={'seed':SEED,'mode':MODE,'format':OUTPUT_FORMAT,'batch_rows':BATCH_ROWS,'workers':WORKERS,'validation':report,'files':files}
    data=json.dumps(manifest,indent=2).encode(); key=prefix+'/manifest.json'
    if STORAGE=='local':
        dest=Path(LOCAL_DIR)/key; tmp=dest.with_name('manifest.json.tmp'); tmp.write_bytes(data); os.replace(tmp,dest)
    else: client.put_object(Bucket=S3_BUCKET,Key=key,Body=data)
    return files

def run_full():
    counts=target_counts(); win={'start':ts(START_DATE),'end':ts(END_DATE)}; capacity_check(counts,win)
    Path(SCRATCH_DIR).mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='full-',dir=SCRATCH_DIR) as work:
        stage=Path(work)/'output'; stage.mkdir(); roots=[t for t in ORDER if not SPECS[t]['fk']]
        if WORKERS>1:
            ctx=mp.get_context('fork' if 'fork' in mp.get_all_start_methods() else 'spawn')
            with ProcessPoolExecutor(max_workers=min(WORKERS,len(roots)),mp_context=ctx) as executor:
                jobs=[executor.submit(full_family,r,counts,str(stage),work,i) for i,r in enumerate(roots)]
                for job in jobs: job.result()
        else:
            for i,r in enumerate(roots): full_family(r,counts,str(stage),work,i)
        con=db_open(Path(work)/'validation.duckdb',Path(work)/'spill')
        try:
            # Parse CSV once, then run repeated checks on compressed disk columns.
            for t in ORDER: con.execute(f'CREATE TABLE {q(tab(t))} AS SELECT * FROM {input_expr(file_path(stage,t),t)}')
            report=validate_db(con,counts,full_window=win,check_parent_lags=True)
        finally: con.close()
        files=publish(stage,S3_PREFIX,report)
    return report,files

def load_existing(con,work):
    counts={}; client=s3_client() if STORAGE=='s3' else None
    for t in ORDER:
        key=S3_PREFIX.strip('/')+'/'+t.split('.')[0]+'/'+t.split('.')[1]+'.'+OUTPUT_FORMAT
        if STORAGE=='local': src=Path(LOCAL_DIR)/key
        else:
            src=Path(work)/('input.'+OUTPUT_FORMAT); client.download_file(S3_BUCKET,key,str(src))
        con.execute(f'CREATE TABLE {q(tab(t))} AS SELECT * FROM {input_expr(src,t)}'); counts[t]=count(con,tab(t))
        if STORAGE=='s3': src.unlink()
    return counts

def select_ids(con,source,dest,n,seed):
    # Sorting narrow random priorities is spillable; do not reservoir-sample full
    # rows or keep O(update/delete count) Python indices in RAM. Fixed seeded hash
    # provides pseudorandom without-replacement selection, independent of scanning.
    query=f'SELECT id FROM {q(source)} WHERE false' if not n else f'SELECT id FROM (SELECT id,row_number() OVER (ORDER BY hash(id,{seed}),id) AS rn FROM {q(source)}) WHERE rn<={n}'
    con.execute(f'CREATE TABLE {q(dest)} AS {query}')

def run_incremental():
    init_pools(SEED); Path(SCRATCH_DIR).mkdir(parents=True,exist_ok=True)
    rng=np.random.default_rng(np.random.SeedSequence([SEED,1]))
    with tempfile.TemporaryDirectory(prefix='incremental-',dir=SCRATCH_DIR) as work:
        stage=Path(work)/'output'; stage.mkdir(); con=db_open(Path(work)/'data.duckdb',Path(work)/'spill')
        try:
            base_counts=load_existing(con,work); validate_db(con)
            minima=[]; maxima=[]
            for t in ORDER:
                cols=[c for c,k in COLS[t] if k=='t']
                row=con.execute('SELECT '+','.join('min('+q(c)+'),max('+q(c)+')' for c in cols)+' FROM '+q(tab(t))).fetchone()
                for i in range(0,len(row),2):
                    if row[i] is not None: minima.append(ts(row[i])); maxima.append(ts(row[i+1]))
            time_lo=min(minima) if minima else ts(START_DATE); max_ts=max(maxima) if maxima else ts(END_DATE)
            start=(max_ts//DAY+1)*DAY; half=(INCR_WINDOW_DAYS//2)*DAY
            if half<5*DAY: raise ValueError('Incremental half-window needs at least five days for event chains')
            upd={'start':start,'end':start+half-1}; ins={'start':start+half,'end':start+2*half}
            insert_counts={}; stats={}
            for t in ORDER:
                f=SPECS[t]['fk']
                insert_counts[t]=0 if t in GOLDEN_TABLES else insert_counts[f[1]]*f[3] if f else round(base_counts[t]*INSERT_PCT/100.)
            capacity_check(insert_counts,ins)
            # Select roots and derive all descendant deletion IDs BEFORE removal.
            for i,t in enumerate(ORDER):
                f=SPECS[t]['fk']; dest='delid'+str(i)
                if t in GOLDEN_TABLES: select_ids(con,tab(t),dest,0,SEED+i)
                elif not f: select_ids(con,tab(t),dest,round(base_counts[t]*DELETE_PCT/100.),SEED+i)
                else: con.execute(f'CREATE TABLE {q(dest)} AS SELECT c.id FROM {q(tab(t))} c JOIN {q("delid"+str(ORDER.index(f[1])))} p ON c.{q(f[0])}=p.id')
            for i in reversed(range(len(ORDER))):
                t=ORDER[i]; dest='deleted'+str(i); ids=q('delid'+str(i)); tb=q(tab(t))
                con.execute(f'CREATE TABLE {q(dest)} AS SELECT b.* FROM {tb} b JOIN {ids} d ON b.id=d.id')
                con.execute(f'DELETE FROM {tb} WHERE id IN (SELECT id FROM {ids})')
            # Process one table at a time: updates only sample surviving baseline
            # rows; parent insert tables exist before corresponding child inserts.
            for i,t in enumerate(ORDER):
                sp=SPECS[t]; tb=q(tab(t)); nt=count(con,tab(t)); k=0 if t in GOLDEN_TABLES else round(nt*UPDATE_PCT/100.)
                capacity_check({t:k},upd)
                ledger=Ledger(Path(work)/('ledger'+str(i)),time_lo,ins['end'])
                try:
                    for df in batches(con,f'SELECT * FROM {tb}'): ledger.seed_frame(t,df)
                    select_ids(con,tab(t),'upid'+str(i),k,SEED+100+i)
                    empty(con,'updated'+str(i),t)
                    query=f'SELECT b.* FROM {tb} b JOIN {q("upid"+str(i))} s ON b.id=s.id ORDER BY b.id'
                    for old in batches(con,query): append(con,'updated'+str(i),generate(t,len(old),rng,upd,ledger,old=old))
                    assignments=','.join(f'{q(c)}=u.{q(c)}' for c in mutable_cols(t)+['modified_d','version'])
                    con.execute(f'UPDATE {tb} b SET {assignments} FROM {q("updated"+str(i))} u WHERE b.id=u.id')
                    empty(con,'inserted'+str(i),t); f=sp['fk']
                    if insert_counts[t] and not f:
                        for off in range(0,insert_counts[t],BATCH_ROWS): append(con,'inserted'+str(i),generate(t,min(BATCH_ROWS,insert_counts[t]-off),rng,ins,ledger))
                    elif insert_counts[t] and f:
                        for pdf in batches(con,f'SELECT id,created_d FROM {q("inserted"+str(ORDER.index(f[1])))} ORDER BY id'):
                            ix=np.repeat(np.arange(len(pdf)),f[3]); rng.shuffle(ix)
                            df=generate(t,len(ix),rng,ins,ledger,pdf.id.to_numpy()[ix],pdf.created_d.to_numpy().astype('datetime64[s]').astype('int64')[ix])
                            append(con,'inserted'+str(i),df)
                    # Assert exact inserts, not merely final relational validity.
                    if count(con,'inserted'+str(i))!=insert_counts[t]: raise AssertionError('Insert count mismatch')
                    con.execute(f'INSERT INTO {tb} SELECT * FROM {q("inserted"+str(i))}')
                    stats[t]={op:count(con,name+str(i)) for op,name in [('DELETE','deleted'),('UPDATE','updated'),('INSERT','inserted')]}
                finally: ledger.close(); shutil.rmtree(ledger.path,ignore_errors=True)
            expected={t:base_counts[t]-stats[t]['DELETE']+stats[t]['INSERT'] for t in ORDER}
            report=validate_db(con,expected); report['operations']=stats
            # All three event chains remain ordered because update timestamps are
            # resampled in the first half, and inserts use the second half only.
            for i,t in enumerate(ORDER):
                delta=INCREMENTAL_OUTPUT=='delta'; writer=Writer(stage,t,delta)
                try:
                    if delta:
                        for op,name in [('DELETE','deleted'),('UPDATE','updated'),('INSERT','inserted')]:
                            for df in batches(con,f'SELECT * FROM {q(name+str(i))}'):
                                df['_operation']=op; writer.write(df)
                    else:
                        for df in batches(con,f'SELECT * FROM {q(tab(t))}'): writer.write(df)
                finally: writer.close()
            files=publish(stage,INCREMENTAL_PREFIX,report)
        finally: con.close()
    return report,files

def read_test_frame(path,t,delta=False):
    df=pd.read_csv(path,dtype={c:'string' for c,k in COLS[t] if k=='s'})
    for c,k in COLS[t]:
        if k in ('t','d'): df[c]=pd.to_datetime(df[c])
    return df

def self_test():
    global STORAGE,MODE,SCALE_FACTOR,BATCH_ROWS,WORKERS,INCREMENTAL_OUTPUT,INCREMENTAL_PREFIX,LOCAL_DIR,SCRATCH_DIR
    STORAGE='local'; MODE='full'; SCALE_FACTOR=1.; BATCH_ROWS=137; WORKERS=2
    # Self-test uses a dedicated local location and never the configured S3 bucket.
    LOCAL_DIR=os.getenv('LC20_TEST_DIR','/tmp/lc20_optimized/test_output'); SCRATCH_DIR=LOCAL_DIR+'/.scratch'
    ROOT_ROW_COUNTS.update({t:1000 for t in ROOT_ROW_COUNTS}); GOLDEN_TABLES.clear(); ROW_COUNT_OVERRIDES.clear()
    baseline_report,baseline_files=run_full()
    MODE='incremental'; INCREMENTAL_OUTPUT='delta'; INCREMENTAL_PREFIX=S3_PREFIX+'incremental/'
    delta_report,delta_files=run_incremental()
    expected={t:2160 if t==D+'das_origination_ext_ref' else 1080 for t in ORDER}
    with tempfile.TemporaryDirectory(prefix='replay-',dir=SCRATCH_DIR) as work:
        con=db_open(Path(work)/'replay.duckdb',Path(work)/'spill')
        try:
            for t in ORDER:
                base=read_test_frame(file_path(Path(LOCAL_DIR)/S3_PREFIX,t),t)
                delta=read_test_frame(file_path(Path(LOCAL_DIR)/INCREMENTAL_PREFIX,t),t,True)
                operations=delta.pop('_operation'); updates=delta[operations=='UPDATE']; inserts=delta[operations=='INSERT']
                mult=2 if t==D+'das_origination_ext_ref' else 1
                assert delta_report['operations'][t]=={'DELETE':20*mult,'UPDATE':49*mult,'INSERT':100*mult}
                assert len(delta)==169*mult
                old=base.set_index('id'); preserved=['created_d']+SPECS[t]['nonmut']+([SPECS[t]['fk'][0]] if SPECS[t]['fk'] else [])
                for c in preserved:
                    a=updates.set_index('id')[c]; b=old.loc[a.index,c]
                    assert np.array_equal(a.to_numpy(),b.to_numpy()),(t,c)
                assert np.array_equal(updates.version.to_numpy(),np.minimum(old.loc[updates.id,'version'].to_numpy()+1,SPECS[t]['ranges']['version'][1]))
                assert np.all(updates.modified_d.to_numpy()>old.loc[updates.id,'modified_d'].to_numpy())
                assert not set(inserts.id).intersection(set(base.id))
                con.register('_base',base); con.execute(f'CREATE TABLE {q(tab(t))} AS SELECT * FROM _base'); con.unregister('_base')
                con.register('_delta',delta.assign(_operation=operations))
                con.execute(f"DELETE FROM {q(tab(t))} WHERE id IN (SELECT id FROM _delta WHERE _operation IN ('DELETE','UPDATE'))")
                cols=','.join(q(c) for c,k in COLS[t])
                con.execute(f"INSERT INTO {q(tab(t))} SELECT {cols} FROM _delta WHERE _operation IN ('UPDATE','INSERT')"); con.unregister('_delta')
            validate_db(con,expected,check_parent_lags=True)
            INCREMENTAL_OUTPUT='merged'; INCREMENTAL_PREFIX=S3_PREFIX+'incremental_merged/'
            merged_report,merged_files=run_incremental()
            for t in ORDER:
                path=file_path(Path(LOCAL_DIR)/INCREMENTAL_PREFIX,t)
                expr=input_expr(path,t)
                # Full symmetric difference verifies delta replay == emitted merged,
                # not just matching counts/constraints.
                sql=f'SELECT count(*) FROM ((SELECT * FROM {q(tab(t))} EXCEPT ALL SELECT * FROM {expr}) UNION ALL (SELECT * FROM {expr} EXCEPT ALL SELECT * FROM {q(tab(t))}))'
                assert con.execute(sql).fetchone()[0]==0,t
            print('DELTA REPLAY == MERGED CSV: all 15 tables')
        finally: con.close()
    # Test finite-domain bookkeeping including repeated byte positions and release.
    with tempfile.TemporaryDirectory(dir=SCRATCH_DIR) as work:
        bm=Bitmap(Path(work)/'test.bits',0,15)
        assert bm.accept([0,1,1,7,8]).tolist()==[True,True,False,True,True]
        bm.release([1,7]); assert bm.accept([0,1,7,8]).tolist()==[False,True,True,False]; bm.close()
    bad={P+'ps_prescreen_entry':10_000_000}
    try: capacity_check(bad,{'start':ts(START_DATE),'end':ts(END_DATE)})
    except ValueError as exc: assert 'offer_amt' in str(exc)
    else: raise AssertionError('Capacity guard missing')
    result={'full':baseline_report,'delta':delta_report,'merged':merged_report,'files':baseline_files,'delta_files':delta_files,'merged_files':merged_files}
    print('SELF TEST PASSED: 1,000 roots; multiple batches; two workers; global constraints; immutable updates; CSV roundtrip; incremental replay; capacity guard')
    return result

def main():
    if STORAGE not in ('local','s3') or MODE not in ('full','incremental') or OUTPUT_FORMAT not in ('csv','parquet') or INCREMENTAL_OUTPUT not in ('delta','merged'): raise ValueError('Invalid mode/storage/format')
    if BATCH_ROWS<1 or WORKERS<1 or DB_THREADS<1: raise ValueError('Batch/workers/threads must be positive')
    if not math.isfinite(SCALE_FACTOR) or SCALE_FACTOR<0 or not (0<=DELETE_PCT<=100 and 0<=UPDATE_PCT<=100 and math.isfinite(INSERT_PCT) and INSERT_PCT>=0): raise ValueError('Invalid scale/operation percentages')
    if SELF_TEST:
        if OUTPUT_FORMAT!='csv': raise ValueError('The self-test intentionally validates preferred CSV output')
        result=self_test()
    else:
        report,files=run_full() if MODE=='full' else run_incremental(); result={'report':report,'files':files}
    summary=Path(LOCAL_DIR)/'validation_summary.json'; summary.parent.mkdir(parents=True,exist_ok=True); summary.write_text(json.dumps(result,indent=2))
    print('Validation summary:',summary)
    return result
if __name__=='__main__': main()
