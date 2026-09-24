# NAMING / DOMAIN RESOLUTIONS
# Catalog wins: as_application_savings_account -> as_application_saving_account;
# applicant_detail.source_code -> source_cd; prescreen tables are lc20_ps;
# all das_* aliases use lc20_das; joint subset/doer1/doer2 are filters, not tables.
# Enum whitespace is stripped; NULL/null means SQL null; 'No Values' means unspecified.
# Application product_type_cd is pinned null. No averages were supplied: assumptions
# below use 1-2 detail/email/process/run rows, one account per matching application,
# 1 applicant for single/null apps, 2 for joint, one origination/member per applicant,
# and one prescreen entry per joint. Joint apps map to a distinct matching single app.
# Reference label exclusions (no generated entity table): listed in EXCLUSIONS below.
# Member loginactorid/pfloginuserid identify EXTERNAL synthetic identity-system users;
# member_ref_id is a seeded UUID, not an FK to an applicant or a generated actor table.
# keyn/keyv fields are synthetic key metadata, not real encryption material.
# Unqualified decimal offer_amt is assumed decimal(20,2). Numeric VARCHARs stay strings.
# Incremental deletion is atomic over primary/joint pairs and dependent rows; derived
# table percentages approximate requested rates because relationship integrity wins.
# _operation is a CDC-only extension, NOT a catalog column. Delta reads strip it;
# a CDC file alone is not a baseline: S3 incremental input must be a merged snapshot.
# UW references stay within their primary/joint application component for coherence.
import io
import random
import tempfile
import uuid
from pathlib import Path
from decimal import Decimal
import numpy as np
import pandas as pd
from faker import Faker
import boto3

SEED = 42
SCALE_FACTOR = 1.0
ROOT_ROW_COUNTS = {'lc20_as.as_application': 1000}
S3_BUCKET = 'mm-fsi-fix'
S3_PREFIX = 'lc/synthetic_data_1/'
OUTPUT_FORMAT = 'csv'
UPLOAD_ENABLED = True
MODE = 'full'
INCREMENTAL_OUTPUT = 'merged'
INSERT_PCT = 10.0
UPDATE_PCT = 5.0
DELETE_PCT = 2.0
DATE_START = '2023-01-01'
DATE_END = '2025-01-01'
DOB_START = '1945-01-01'
DOB_END = '2001-01-01'
CHILD_MIN = 1
CHILD_MAX = 2
AVG_CHILDREN = 1.5
AUDIT_DAY_RANGE = (3, 30)
CHILD_SECOND_RANGE = (1, 3600)
VERSION_RANGE = (1, 9)
KEY_VERSION_RANGE = (1, 5)
APY_CENTS_RANGE = (100, 550)
TERM_MONTHS = [6, 12, 18, 24, 36, 60]
DEPOSIT_CENTS_RANGE = (10000, 10000000)
MIN_DEPOSIT_CENTS = [2500, 5000, 10000]
INCOME_RANGE = (15000, 250000)
ADDITIONAL_INCOME_RANGE = (0, 30000)
HOUSING_RANGE = (3000, 50000)
DEBT_RANGE = (0, 20000)
JOB_MONTH_RANGE = (0, 240)
FICO_BANDS = [(300, 579), (580, 669), (670, 739), (740, 799), (800, 850)]
HONORIFICS = ['Mr', 'Ms', 'Mx', 'Dr']
MARITAL = ['SINGLE', 'MARRIED', 'DIVORCED', 'WIDOWED']
EDUCATION = ['HIGH_SCHOOL', 'ASSOCIATE', 'BACHELOR', 'GRADUATE']
ATTRIBUTION = ['ORGANIC', 'SEARCH', 'REFERRAL', 'BRANCH']
KEY_NAMES = ['synthetic-key-a', 'synthetic-key-b', 'synthetic-key-c']
PS_STATUSES = ['ACTIVE', 'USED', 'EXPIRED']
PS_PURPOSES = ['SAVINGS', 'CHECKING', 'CERTIFICATE']
POLICIES = ['DEPOSIT_STANDARD', 'DEPOSIT_PRIME', 'DEPOSIT_GENERAL']
PS_VALID_DAYS = 90
UW_STATUS_OFFSET_SECONDS = 3600
UW_DECISION_OFFSET_SECONDS = 7200
APP_TYPES = ['SAVINGS_ACCOUNT', 'JOINT_SAVINGS_ACCOUNT', 'CHECKING_ACCOUNT', 'JOINT_CHECKING_ACCOUNT', 'CERTIFICATE_DEPOSIT_ACCOUNT', 'JOINT_CERTIFICATE_DEPOSIT_ACCOUNT', None]
PHASES = ['FULL_DECISION', 'ADDRESS_VALIDATION', 'BANK_ACCOUNT', 'FULFILLMENT', None]
OP_TOLERANCE_FRACTION = 0.45
OP_TOLERANCE_ROWS = 6
MONEY_QUANTUM = Decimal('0.01')
NULL_SENTINELS = {'None', 'nan', 'NULL'}
# Literal catalog identifiers and types, losslessly regrouped into table blocks.
CATALOG_TEXT = '''lc20_as.as_application|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,application_d:timestamp(3),product_type_cd:varchar(2147483647),application_type_cd:varchar(2147483647)
lc20_as.as_applicant|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,application_id:varchar(2147483647),primary_applicant_flg:tinyint,sms_marketing_opt_in_flg:tinyint,marketing_attribution_cd:varchar(2147483647)
lc20_as.as_application_saving_account|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,source_cd:varchar(2147483647),application_id:varchar(2147483647),initial_deposit_amount:varchar(2147483647),product_code:varchar(2147483647),is_joint_flg:tinyint,debit_card_requested_flg:tinyint,account_type:varchar(2147483647)
lc20_as.as_application_checking_account|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,application_id:varchar(2147483647),source_cd:varchar(2147483647),debit_card_requested_flg:tinyint,min_initial_deposit_amount:varchar(2147483647),initial_deposit_amount:varchar(2147483647),product_code:varchar(2147483647),is_joint_flg:tinyint,account_type:varchar(2147483647)
lc20_as.as_application_certificate_of_deposit_account|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,source_cd:varchar(2147483647),initial_deposit_amount:varchar(2147483647),product_code:varchar(2147483647),application_id:varchar(2147483647),apy:decimal(20;2),term_in_months:integer,is_joint_flg:tinyint,account_type:varchar(2147483647)
lc20_as.as_applicant_detail|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,applicant_id:varchar(2147483647),source_cd:varchar(2147483647),first_name:varchar(2147483647),first_name_keyn:varchar(2147483647),first_name_keyv:integer,last_name:varchar(2147483647),last_name_keyn:varchar(2147483647),last_name_keyv:integer,honorific_cd:varchar(2147483647),date_of_birth:varchar(2147483647),date_of_birth_keyn:varchar(2147483647),date_of_birth_keyv:integer,ssn:varchar(2147483647),ssn_keyn:varchar(2147483647),ssn_keyv:integer,marital_status_cd:varchar(2147483647),educational_qualification_cd:varchar(2147483647),employment_status_cd:varchar(2147483647),cumulative_job_tenure_months:varchar(2147483647),annual_income:varchar(2147483647),annual_additional_income:varchar(2147483647),annual_income_taxable:varchar(2147483647),annual_income_non_taxable:varchar(2147483647),housing_status_cd:varchar(2147483647),annual_housing_expense:varchar(2147483647),annual_additional_debt:varchar(2147483647),fico_range_cd:varchar(2147483647),fico_min:varchar(2147483647),fico_max:varchar(2147483647),citizenship:tinyint
lc20_as.as_email_address|id:varchar(2147483647),created_d:timestamp(3),modified_d:timestamp(3),version:bigint,applicant_id:varchar(2147483647),source_cd:varchar(2147483647),email_address:varchar(2147483647),email_address_keyn:varchar(2147483647),email_address_keyv:integer
lc20_uw_processing.uw_process|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,subject_ref_id:string,subject_entity_name:string,type_cd:string,subject_src_id:string,status_cd:string,product_type_cd:string,status_d:timestamp,decision_status_cd:string,decision_status_d:timestamp,decision_phase_cd:string,subject_type_cd:string,expiration_d:timestamp
lc20_uw_processing.uw_process_run|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,uw_process_id:string,decision_phase_cd:string,decision_status_cd:string
lc20_uw_processing.uw_process_run_ext_ref|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,uw_process_run_id:string,ext_ref_id:string,ext_src_id:string,ext_ref_entity_name:string
lc20_das.das_origination|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,origination_guid:string
lc20_das.das_member_origination|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,origination_id:string,member_ref_id:string,member_src_id:string,member_entity_name:string
lc20_das.das_origination_ext_ref|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,origination_id:string,ext_ref_id:string,ext_src_id:string,ext_entity_name:string,flow_id:string,flow_version:smallint
lc20_ps.ps_prescreen_entry|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,source_cd:string,credit_policy_name:string,credit_policy_version:smallint,effective_start_d:timestamp,effective_end_d:timestamp,status_cd:string,first_look_up:timestamp,last_look_up:timestamp,used_d:timestamp,offer_amt:decimal,offer_purpose_cd:string
lc20_ps.ps_prescreen_ext_ref|id:string,created_d:timestamp,modified_d:timestamp,version:bigint,ps_prescreen_entry_id:string,ext_src_id:string,ext_ref_id:string,ext_entity_name:string,ext_ref_direction_cd:string'''
# Semicolon inside decimal(20;2) is solely a delimiter escape for the literal catalog.
ENUM_TEXT = '''lc20_as.as_applicant_detail|source_code|APPLICANT
lc20_as.as_applicant_detail|honorific_cd|No Values
lc20_as.as_applicant_detail|marital_status_cd|No Values
lc20_as.as_applicant_detail|educational_qualification_cd|No Values
lc20_as.as_applicant_detail|employment_status_cd|UNEMPLOYED,EMPLOYED,RETIRED,SELFEMPLOYED,DISABLED,OTHER,NULL
lc20_as.as_applicant_detail|housing_status_cd|OWN,MORTGAGE,RENT,NONE,ANY,OTHER,NULL
lc20_as.as_applicant_detail|fico_range_cd|No Values
lc20_as.as_application|product_type_cd|should be null then you will get below application_type_cd
lc20_as.as_application|application_type_cd|SAVINGS_ACCOUNT,CHECKING_ACCOUNT,CERTIFICATE_DEPOSIT_ACCOUNT,JOINT_SAVINGS_ACCOUNT,JOINT_CERTIFICATE_DEPOSIT_ACCOUNT,JOINT_CHECKING_ACCOUNT,NULL
lc20_as.as_applicant|marketing_attribution_cd|No Values
lc20_as.as_application_certificate_of_deposit_account|source_cd|APPLICANT
lc20_as.as_application_certificate_of_deposit_account|product_code|14,26,25,22,06,12,21,23,28
lc20_as.as_application_certificate_of_deposit_account|account_type|CD
lc20_as.as_application_checking_account|source_cd|APPLICANT
lc20_as.as_application_checking_account|product_code|53,01
lc20_as.as_application_checking_account|account_type|ND
lc20_as.as_application_savings_account|source_cd|APPLICANT
lc20_as.as_application_savings_account|product_code|09,10,25
lc20_as.as_application_savings_account|account_type|SV
lc20_as.as_email_address|source_cd|APPLICANT
lc20_das.das_member_origination|member_src_id|1000,1200
lc20_das.das_member_origination|member_entity_name|loginactorid,pfloginuserid
lc20_das.das_origination_ext_ref|ext_entity_name|prescreenentry,bpsrequestid,borroweractorid,loanapp,applicantid,coapplicantid,applicationid,address,coborroweractorid,actor,depositaccount
lc20_ps.ps_prescreen_ext_ref|ext_entity_name|actor,loan,referrer,product,bpsrequestid,loanapp,loginactorid,bankaccount,productcode,accounttype,cif,applicationid,program
lc20_ps.ps_prescreen_ext_ref|ext_ref_direction_cd|TARGET,SOURCE,NULL
lc20_uw_processing.uw_process|subject_entity_name|applicationid,address ,actor
lc20_uw_processing.uw_process|type_cd|APPLICATION_DECISION,ADDRESS_VALIDATION,null
lc20_uw_processing.uw_process|status_cd|FAILURE,CANCELED,SUCCESS,EXPIRED,WITHDRAWN,ACTIVE ,NEW
lc20_uw_processing.uw_process|product_type_cd|No Values
lc20_uw_processing.uw_process|decision_status_cd|null   ,FAILURE,ERROR  ,IN_PROGRESS,SUCCESS,MORE_INFO,PENDING
lc20_uw_processing.uw_process_run|decision_phase_cd|FULL_DECISION,ADDRESS_VALIDATION,BANK_ACCOUNT ,FULFILLMENT  ,null
lc20_uw_processing.uw_process_run|decision_status_cd|FAILURE,IN_PROGRESS,SUCCESS,MORE_INFO  ,null   ,ERROR  ,PENDING
lc20_uw_processing.uw_process_run_ext_ref|ext_ref_entity_name|narmiuuid   ,bankaccount ,prescreenentry,journeyapplicationtoken,applicantid ,cif,loginactorid,entitytoken ,offerid'''
ALIASES = {'lc20_as.as_application_savings_account': 'lc20_as.as_application_saving_account'}
EXCLUSIONS = {
 ('lc20_das.das_origination_ext_ref','ext_entity_name'): {'bpsrequestid','borroweractorid','loanapp','address','coborroweractorid','actor'},
 ('lc20_ps.ps_prescreen_ext_ref','ext_entity_name'): {'actor','loan','referrer','product','bpsrequestid','loanapp','loginactorid','bankaccount','productcode','accounttype','cif','program'},
 ('lc20_uw_processing.uw_process','subject_entity_name'): {'address','actor'},
 ('lc20_uw_processing.uw_process_run_ext_ref','ext_ref_entity_name'): {'narmiuuid','bankaccount','journeyapplicationtoken','cif','loginactorid','entitytoken','offerid'} }
A='lc20_as.as_application'
P='lc20_as.as_applicant'
D='lc20_as.as_applicant_detail'
E='lc20_as.as_email_address'
S='lc20_as.as_application_saving_account'
C='lc20_as.as_application_checking_account'
CD='lc20_as.as_application_certificate_of_deposit_account'
O='lc20_das.das_origination'
M='lc20_das.das_member_origination'
X='lc20_das.das_origination_ext_ref'
PS='lc20_ps.ps_prescreen_entry'
PX='lc20_ps.ps_prescreen_ext_ref'
U='lc20_uw_processing.uw_process'
R='lc20_uw_processing.uw_process_run'
RX='lc20_uw_processing.uw_process_run_ext_ref'
ACCOUNT_CONFIG = {S: ('SAVINGS_ACCOUNT','SV'), C: ('CHECKING_ACCOUNT','ND'), CD: ('CERTIFICATE_DEPOSIT_ACCOUNT','CD')}
ORDER = [A,P,S,C,CD,D,E,O,M,PS,X,PX,U,R,RX]
FKS = [(P,'application_id',A),(S,'application_id',A),(C,'application_id',A),(CD,'application_id',A),(D,'applicant_id',P),(E,'applicant_id',P),(M,'origination_id',O),(X,'origination_id',O),(PX,'ps_prescreen_entry_id',PS),(U,'subject_ref_id',A),(R,'uw_process_id',U),(RX,'uw_process_run_id',R)]
RESERVED_PS = {('applicationid','11004','SOURCE')}
PINNED = {(A,'product_type_cd'),(U,'subject_src_id'),(U,'subject_type_cd'),(PS,'source_cd'),(X,'flow_id'),(PX,'ext_src_id')}
NULLABLE_EXTRA = {(U,'decision_phase_cd'),(U,'decision_status_d'),(PS,'used_d')}

def parse_catalog():
    return {t: dict(item.split(':',1) for item in cols.replace(';', '~').split(',')) for t,cols in (line.split('|') for line in CATALOG_TEXT.splitlines())}
SCHEMA = parse_catalog()

def parse_enums():
    result={}
    for line in ENUM_TEXT.splitlines():
        t,c,values=line.split('|'); t=ALIASES.get(t,t)
        if c=='source_code': c='source_cd'
        assert t in SCHEMA and c in SCHEMA[t]
        if values=='No Values': continue
        domain=[None] if values.startswith('should be null') else [None if v.strip().lower()=='null' else v.strip() for v in values.split(',')]
        result[t,c]=domain
    return result
ENUMS=parse_enums()

def seed_sources():
    random.seed(SEED); Faker.seed(SEED)
    fake=Faker('en_US'); fake.seed_instance(SEED)
    return np.random.default_rng(SEED),fake

class Allocator:
    def __init__(self, existing=None):
        self.next={t: (max((int(x) for x in existing[t].id),default=0)+1 if existing is not None else 1) for t in SCHEMA}
    def take(self,t):
        value=self.next[t]; self.next[t]+=1
        return str(value)

def pick(rng,values):
    return values[int(rng.integers(len(values)))]

def enum_pick(rng,t,c):
    return pick(rng,[v for v in ENUMS[t,c] if v not in EXCLUSIONS.get((t,c),set())])

def money(cents):
    return format((Decimal(int(cents))/100).quantize(MONEY_QUANTUM),'f')

def base_row(t,alloc,rng,parent_time=None):
    if parent_time is None:
        created=pd.Timestamp(DATE_START)+pd.Timedelta(seconds=int(rng.integers(0,int((pd.Timestamp(DATE_END)-pd.Timestamp(DATE_START)).total_seconds()))))
    else:
        created=parent_time+pd.Timedelta(seconds=int(rng.integers(*CHILD_SECOND_RANGE)))
    return {'id':alloc.take(t),'created_d':created,'modified_d':created+pd.Timedelta(days=int(rng.integers(*AUDIT_DAY_RANGE))), 'version':int(rng.integers(*VERSION_RANGE))}

def child_counts(n,rng):
    assert CHILD_MIN <= AVG_CHILDREN <= CHILD_MAX
    counts=np.full(n,CHILD_MIN,dtype=int)
    extra=int(round(n*AVG_CHILDREN))-n*CHILD_MIN
    while extra:
        eligible=np.flatnonzero(counts<CHILD_MAX)
        take=min(extra,len(eligible)); chosen=rng.choice(eligible,size=take,replace=False)
        counts[chosen]+=1; extra-=take
    return counts

def metadata(row,fields,rng):
    for c in fields:
        row[c+'_keyn']=pick(rng,KEY_NAMES); row[c+'_keyv']=int(rng.integers(*KEY_VERSION_RANGE))

def link_foreign_keys(row,column,parent):
    row[column]=parent['id']; return row

def enforce_types(t,records):
    df=pd.DataFrame(records,columns=list(SCHEMA[t]))
    for c,typ in SCHEMA[t].items():
        if typ.startswith('timestamp'): df[c]=pd.to_datetime(df[c]).astype('datetime64[ns]')
        elif typ in ('bigint','integer','smallint','tinyint'): df[c]=df[c].astype('int64')
        elif typ.startswith('decimal'): df[c]=df[c].map(lambda v: Decimal(v).quantize(MONEY_QUANTUM))
        else: df[c]=df[c].astype('string')
    return df

def generate_applications(n,alloc,rng):
    rows=[]; joint_to_primary={}; latest={}
    for i in range(n):
        row=base_row(A,alloc,rng); kind=APP_TYPES[i%len(APP_TYPES)]
        row.update(application_type_cd=kind,product_type_cd=None,application_d=row['created_d'])
        if kind and kind.startswith('JOINT_'): joint_to_primary[row['id']]=latest[kind.removeprefix('JOINT_')]
        elif kind: latest[kind]=row['id']
        rows.append(row)
    return rows,joint_to_primary

def generate_applicants(apps,alloc,rng):
    rows=[]
    for app in apps:
        count=2 if (app['application_type_cd'] or '').startswith('JOINT_') else 1
        for j in range(count):
            row=link_foreign_keys(base_row(P,alloc,rng,app['created_d']),'application_id',app)
            row.update(primary_applicant_flg=int(j==0),sms_marketing_opt_in_flg=int(rng.integers(2)),marketing_attribution_cd=pick(rng,ATTRIBUTION)); rows.append(row)
    return rows

def generate_accounts(t,apps,alloc,rng):
    rows=[]; kind,account_type=ACCOUNT_CONFIG[t]
    for app in apps:
        if app['application_type_cd'] not in (kind,'JOINT_'+kind): continue
        row=link_foreign_keys(base_row(t,alloc,rng,app['created_d']),'application_id',app)
        row.update(source_cd='APPLICANT',initial_deposit_amount=money(rng.integers(*DEPOSIT_CENTS_RANGE)),product_code=enum_pick(rng,t,'product_code'),is_joint_flg=int(app['application_type_cd'].startswith('JOINT_')),account_type=account_type)
        if t in (S,C): row['debit_card_requested_flg']=int(rng.integers(2))
        if t==C: row['min_initial_deposit_amount']=money(pick(rng,MIN_DEPOSIT_CENTS))
        if t==CD: row.update(apy=(Decimal(int(rng.integers(*APY_CENTS_RANGE)))/100).quantize(MONEY_QUANTUM),term_in_months=pick(rng,TERM_MONTHS))
        rows.append(row)
    return rows

def generate_person_children(t,people,alloc,rng,fake):
    rows=[]
    for p,count in zip(people,child_counts(len(people),rng)):
        # Repeated applicant details describe the same synthetic person.
        first=fake.first_name(); last=fake.last_name(); ssn=fake.ssn()
        dob=fake.date_between_dates(date_start=pd.Timestamp(DOB_START).date(),date_end=pd.Timestamp(DOB_END).date()).isoformat()
        for _ in range(count):
            row=link_foreign_keys(base_row(t,alloc,rng,p['created_d']),'applicant_id',p); row['source_cd']='APPLICANT'
            if t==E:
                row['email_address']=fake.email(); metadata(row,['email_address'],rng)
            else:
                income=int(rng.integers(*INCOME_RANGE)); additional=int(rng.integers(*ADDITIONAL_INCOME_RANGE)); low,high=pick(rng,FICO_BANDS)
                employment=enum_pick(rng,D,'employment_status_cd')
                row.update(first_name=first,last_name=last,ssn=ssn,date_of_birth=dob,honorific_cd=pick(rng,HONORIFICS),marital_status_cd=pick(rng,MARITAL),educational_qualification_cd=pick(rng,EDUCATION),employment_status_cd=employment,cumulative_job_tenure_months=str(int(rng.integers(*JOB_MONTH_RANGE))),annual_income=str(income),annual_additional_income=str(additional),annual_income_taxable=str(income),annual_income_non_taxable=str(additional),housing_status_cd=enum_pick(rng,D,'housing_status_cd'),annual_housing_expense=str(int(rng.integers(*HOUSING_RANGE))),annual_additional_debt=str(int(rng.integers(*DEBT_RANGE))),fico_range_cd=f'{low}-{high}',fico_min=str(low),fico_max=str(high),citizenship=int(rng.integers(2)))
                metadata(row,['first_name','last_name','date_of_birth','ssn'],rng)
            rows.append(row)
    return rows

def generate_originations_and_prescreens(rows,joint_map,alloc,rng,fake):
    apps={a['id']:a for a in rows[A]}; primary={p['application_id']:p for p in rows[P] if p['primary_applicant_flg']==1}; origins={}
    def add_ref(origin,target,label,source):
        row=base_row(X,alloc,rng,max(origin['created_d'],target['created_d']))
        row.update(origination_id=origin['id'],ext_ref_id=target['id'],ext_src_id=source,ext_entity_name=label,flow_id='DEPOSIT_ORIGINATION',flow_version=int(rng.integers(*KEY_VERSION_RANGE))); rows[X].append(row)
    for p in rows[P]:
        origin=base_row(O,alloc,rng,p['created_d']); origin['origination_guid']=str(uuid.uuid5(uuid.NAMESPACE_URL,f'{SEED}/{O}/{origin["id"]}')); rows[O].append(origin); origins[p['id']]=origin
        member=base_row(M,alloc,rng,origin['created_d']); source=enum_pick(rng,M,'member_src_id')
        member.update(origination_id=origin['id'],member_ref_id=str(uuid.uuid5(uuid.NAMESPACE_URL,f'{SEED}/{M}/{member["id"]}')),member_src_id=source,member_entity_name='loginactorid' if source=='1000' else 'pfloginuserid'); rows[M].append(member)
        add_ref(origin,p,'applicantid','11004')
        if not p['primary_applicant_flg']: add_ref(origin,p,'coapplicantid','11004')
    for t in ACCOUNT_CONFIG:
        for account in rows[t]: add_ref(origins[primary[account['application_id']]['id']],account,'depositaccount', {S:'11005',C:'11006',CD:'11007'}[t])
    for joint_id,parent_id in joint_map.items():
        joint=apps[joint_id]; parent=apps[parent_id]; origin=origins[primary[joint_id]['id']]
        ps=base_row(PS,alloc,rng,max(origin['created_d'],parent['created_d'])); created=ps['created_d']; status=pick(rng,PS_STATUSES)
        ps.update(source_cd='APPLICANT',credit_policy_name=pick(rng,POLICIES),credit_policy_version=int(rng.integers(*KEY_VERSION_RANGE)),effective_start_d=created,effective_end_d=created+pd.Timedelta(days=PS_VALID_DAYS),status_cd=status,first_look_up=created+pd.Timedelta(seconds=UW_STATUS_OFFSET_SECONDS),last_look_up=created+pd.Timedelta(seconds=UW_DECISION_OFFSET_SECONDS),used_d=created+pd.Timedelta(seconds=UW_DECISION_OFFSET_SECONDS) if status=='USED' else None,offer_amt=Decimal(money(rng.integers(*DEPOSIT_CENTS_RANGE))),offer_purpose_cd=pick(rng,PS_PURPOSES)); rows[PS].append(ps)
        add_ref(origin,joint,'applicationid','11004'); add_ref(origin,ps,'prescreenentry','11000')
        for direction in ENUMS[PX,'ext_ref_direction_cd']:
            target=parent if direction=='SOURCE' else joint
            combination=('applicationid','11004',direction)
            if direction!='SOURCE': assert combination not in RESERVED_PS
            ref=base_row(PX,alloc,rng,max(ps['created_d'],target['created_d']))
            ref.update(ps_prescreen_entry_id=ps['id'],ext_src_id='11004',ext_ref_id=target['id'],ext_entity_name='applicationid',ext_ref_direction_cd=direction); rows[PX].append(ref)

def generate_underwriting(rows,alloc,rng):
    for app,count in zip(rows[A],child_counts(len(rows[A]),rng)):
        for _ in range(count):
            u=base_row(U,alloc,rng,app['created_d']); decision=enum_pick(rng,U,'decision_status_cd')
            u.update(subject_ref_id=app['id'],subject_entity_name='applicationid',type_cd=enum_pick(rng,U,'type_cd'),subject_src_id='11004',status_cd=enum_pick(rng,U,'status_cd'),product_type_cd=pick(rng,PS_PURPOSES),status_d=u['created_d']+pd.Timedelta(seconds=UW_STATUS_OFFSET_SECONDS),decision_status_cd=decision,decision_status_d=u['created_d']+pd.Timedelta(seconds=UW_DECISION_OFFSET_SECONDS) if decision else None,decision_phase_cd=pick(rng,PHASES),subject_type_cd='DEPOSIT_APPLICATION',expiration_d=u['created_d']+pd.Timedelta(days=PS_VALID_DAYS)); rows[U].append(u)
    for u,count in zip(rows[U],child_counts(len(rows[U]),rng)):
        for _ in range(count):
            run=base_row(R,alloc,rng,u['created_d']); run.update(uw_process_id=u['id'],decision_phase_cd=enum_pick(rng,R,'decision_phase_cd'),decision_status_cd=enum_pick(rng,R,'decision_status_cd')); rows[R].append(run)
    process_app={u['id']:u['subject_ref_id'] for u in rows[U]}
    people_by_app={a['id']:[] for a in rows[A]}
    for person in rows[P]: people_by_app[person['application_id']].append(person)
    ps_by_id={p['id']:p for p in rows[PS]}
    ps_by_app={ref['ext_ref_id']:ps_by_id[ref['ps_prescreen_entry_id']] for ref in rows[PX]}
    for run,count in zip(rows[R],child_counts(len(rows[R]),rng)):
        app_id=process_app[run['uw_process_id']]
        for _ in range(count):
            labels=[v for v in ENUMS[RX,'ext_ref_entity_name'] if v not in EXCLUSIONS[RX,'ext_ref_entity_name'] and (v!='prescreenentry' or app_id in ps_by_app)]
            label=pick(rng,labels)
            target=pick(rng,people_by_app[app_id]) if label=='applicantid' else ps_by_app[app_id]
            ref=base_row(RX,alloc,rng,max(run['created_d'],target['created_d']))
            ref.update(uw_process_run_id=run['id'],ext_ref_id=target['id'],ext_src_id='11004' if label=='applicantid' else '11000',ext_ref_entity_name=label); rows[RX].append(ref)

def generate_full(rng,fake,alloc=None,n=None):
    alloc=alloc or Allocator(); n=int(round(ROOT_ROW_COUNTS[A]*SCALE_FACTOR)) if n is None else n
    assert n>=len(APP_TYPES), 'At least seven applications per insert batch required for complete chains'
    rows={t:[] for t in SCHEMA}; rows[A],mapping=generate_applications(n,alloc,rng)
    rows[P]=generate_applicants(rows[A],alloc,rng)
    for t in ACCOUNT_CONFIG: rows[t]=generate_accounts(t,rows[A],alloc,rng)
    for t in (D,E): rows[t]=generate_person_children(t,rows[P],alloc,rng,fake)
    generate_originations_and_prescreens(rows,mapping,alloc,rng,fake); generate_underwriting(rows,alloc,rng)
    return {t:enforce_types(t,rows[t]) for t in ORDER}

def primary_chain(tables):
    x=tables[X]; px=tables[PX]
    joint=x[(x.ext_entity_name=='applicationid') & (x.ext_src_id=='11004')][['ext_ref_id','origination_id']].rename(columns={'ext_ref_id':'joint_id'})
    ps=x[(x.ext_entity_name=='prescreenentry') & (x.ext_src_id=='11000')][['origination_id','ext_ref_id']].rename(columns={'ext_ref_id':'ps_id'})
    source=px[(px.ext_entity_name=='applicationid') & (px.ext_src_id=='11004') & (px.ext_ref_direction_cd=='SOURCE')][['ps_prescreen_entry_id','ext_ref_id']].rename(columns={'ext_ref_id':'primary_id'})
    return joint.merge(ps,on='origination_id',validate='one_to_one').merge(source,left_on='ps_id',right_on='ps_prescreen_entry_id',validate='one_to_one')

CHECK_TOTAL=0

def validate(tables,label,expected_root=None,full_targets=False):
    global CHECK_TOTAL
    checks=[]
    def check(name,predicate):
        assert bool(predicate), f'{label}: {name}'
        checks.append(name)
    literal=parse_catalog()
    check('literal catalog table set',set(tables)==set(literal))
    for t,df in tables.items():
        check(t+' literal columns',list(df)==list(literal[t]))
        check(t+' PK required unique',df.id.notna().all() and df.id.is_unique)
        check(t+' audit ordering',(df.modified_d>=df.created_d).all())
        for c,typ in literal[t].items():
            values=df[c]; key=(t,c)
            nullable=(None in ENUMS.get(key,[])) or key in NULLABLE_EXTRA
            if not nullable: check(t+'.'+c+' required',values.notna().all())
            check(t+'.'+c+' null tokens',not any(isinstance(v,str) and v in NULL_SENTINELS for v in values.dropna()))
            if typ.startswith(('varchar','string')): check(t+'.'+c+' string fidelity',all(isinstance(v,str) for v in values.dropna()))
            elif typ.startswith('timestamp'): check(t+'.'+c+' timestamp fidelity',pd.api.types.is_datetime64_any_dtype(values) and (values.dropna().astype('int64')%1000000==0).all())
            elif typ.startswith('decimal'): check(t+'.'+c+' Decimal scale precision',all(isinstance(v,Decimal) and v.as_tuple().exponent==-2 and abs(v)<Decimal('1e18') for v in values))
            else: check(t+'.'+c+' integer fidelity',pd.api.types.is_integer_dtype(values))
            if typ=='tinyint': check(t+'.'+c+' flag range',values.isin([0,1]).all())
            if key in ENUMS:
                observed=set(None if pd.isna(v) else v for v in values)
                domain=set(ENUMS[key])-EXCLUSIONS.get(key,set())
                check(t+'.'+c+' exact enum domain membership',observed<=domain)
                if len(df)>=100: check(t+'.'+c+' full enum coverage',observed==domain)
            domain=set(ENUMS.get(key,[]))-EXCLUSIONS.get(key,set())
            pinned=key in PINNED or len(domain)==1
            if len(df)>=100 and not pinned: check(t+'.'+c+' nonconstant',values.nunique(dropna=False)>1)
        print('DIVERSITY',label,t,df.nunique(dropna=False).to_dict())
    for child,fk,parent in FKS:
        check(child+'.'+fk+' FK',tables[child][fk].isin(tables[parent].id).all())
        joined=tables[child].merge(tables[parent][['id','created_d']],left_on=fk,right_on='id',suffixes=('','_parent'),validate='many_to_one')
        check(child+'.'+fk+' temporal',(joined.created_d>=joined.created_d_parent).all())
    apps=tables[A]; people=tables[P]; kinds=apps.set_index('id').application_type_cd
    if expected_root is not None: check('root count target',len(apps)==expected_root)
    check('application date ordering',(apps.application_d<=apps.created_d).all())
    check('applicant exact application coverage',set(people.application_id)==set(apps.id))
    counts=people.groupby('application_id').size(); expected=kinds.map(lambda k: 2 if pd.notna(k) and k.startswith('JOINT_') else 1)
    check('applicant conditional count',counts.sort_index().equals(expected.astype('int64').sort_index()))
    check('one primary applicant per app',(people.groupby('application_id').primary_applicant_flg.sum()==1).all())
    for t,(kind,account_type) in ACCOUNT_CONFIG.items():
        df=tables[t]; desired=set(apps.loc[apps.application_type_cd.isin([kind,'JOINT_'+kind]),'id'])
        check(t+' conditional exact 1:1',df.application_id.is_unique and set(df.application_id)==desired)
        check(t+' joint flag coherence',(df.is_joint_flg==df.application_id.map(kinds).str.startswith('JOINT_').astype(int)).all())
        amounts=df.initial_deposit_amount.map(Decimal)
        check(t+' deposit range',amounts.between(Decimal(DEPOSIT_CENTS_RANGE[0])/100,Decimal(DEPOSIT_CENTS_RANGE[1])/100).all())
        if t==C: check('checking minimum <= actual',(df.min_initial_deposit_amount.map(Decimal)<=amounts).all())
        if t==CD:
            check('CD APY range',df.apy.map(lambda v: Decimal(APY_CENTS_RANGE[0])/100<=v<Decimal(APY_CENTS_RANGE[1])/100).all())
            check('CD term domain',df.term_in_months.isin(TERM_MONTHS).all())
    for t,fk,parent in [(D,'applicant_id',P),(E,'applicant_id',P),(U,'subject_ref_id',A),(R,'uw_process_id',U),(RX,'uw_process_run_id',R)]:
        cc=tables[t].groupby(fk).size()
        check(t+' exact parent coverage',set(cc.index)==set(tables[parent].id))
        check(t+' child min/max',cc.between(CHILD_MIN,CHILD_MAX).all())
        if full_targets: check(t+' derived row target',len(tables[t])==round(len(tables[parent])*AVG_CHILDREN))
    detail=tables[D]
    check('FICO min/max/band',all(int(r.fico_min)<=int(r.fico_max) and r.fico_range_cd==f'{r.fico_min}-{r.fico_max}' for r in detail.itertuples()))
    check('income components',all(Decimal(r.annual_income)+Decimal(r.annual_additional_income)==Decimal(r.annual_income_taxable)+Decimal(r.annual_income_non_taxable) for r in detail.itertuples()))
    check('DOB before applicant creation',(pd.to_datetime(detail.date_of_birth)<detail.created_d).all())
    x=tables[X]; ps=tables[PS]; px=tables[PX]; orig=tables[O]; members=tables[M]
    semantic=[(X,'ext_entity_name','ext_ref_id',{'applicationid':A,'prescreenentry':PS,'applicantid':P,'coapplicantid':P}),(PX,'ext_entity_name','ext_ref_id',{'applicationid':A}),(U,'subject_entity_name','subject_ref_id',{'applicationid':A}),(RX,'ext_ref_entity_name','ext_ref_id',{'applicantid':P,'prescreenentry':PS})]
    for t,lc,rc,mapping in semantic:
        for value,target in mapping.items():
            part=tables[t][tables[t][lc]==value]
            check(t+' semantic '+value,part[rc].isin(tables[target].id).all())
            joined=part.merge(tables[target][['id','created_d']],left_on=rc,right_on='id',suffixes=('','_target'))
            check(t+' reference time '+value,(joined.created_d>=joined.created_d_target).all())
    for t,src in [(S,'11005'),(C,'11006'),(CD,'11007')]:
        part=x[(x.ext_entity_name=='depositaccount') & (x.ext_src_id==src)]
        check('depositaccount '+t+' exact semantic coverage',part.ext_ref_id.is_unique and set(part.ext_ref_id)==set(tables[t].id))
    check('depositaccount source domain',x.loc[x.ext_entity_name=='depositaccount','ext_src_id'].isin(['11005','11006','11007']).all())
    applicant_refs=x[x.ext_entity_name=='applicantid']
    check('actor path exact applicant coverage',applicant_refs.ext_ref_id.is_unique and set(applicant_refs.ext_ref_id)==set(people.id))
    check('one origination per applicant',applicant_refs.origination_id.is_unique and set(applicant_refs.origination_id)==set(orig.id))
    check('one member per origination',members.origination_id.is_unique and set(members.origination_id)==set(orig.id))
    actor=applicant_refs.merge(members,on='origination_id',validate='one_to_one')
    check('actor chain distinct external identities',len(actor)==len(people) and actor.member_ref_id.is_unique)
    check('member source/entity coherence',(members.member_entity_name==members.member_src_id.map({'1000':'loginactorid','1200':'pfloginuserid'})).all())
    check('coapplicant exact coverage',set(x.loc[x.ext_entity_name=='coapplicantid','ext_ref_id'])==set(people.loc[people.primary_applicant_flg==0,'id']))
    chain=primary_chain(tables); joint_ids=set(apps.loc[apps.application_type_cd.fillna('').str.startswith('JOINT_'),'id'])
    check('joint subset chain exact coverage',chain.joint_id.is_unique and set(chain.joint_id)==joint_ids)
    check('joint prescreen N:1 distinct coverage',chain.ps_id.is_unique and set(chain.ps_id)==set(ps.id))
    check('joint distinct varying primary targets',chain.primary_id.is_unique and (chain.joint_id!=chain.primary_id).all())
    check('joint matching primary type',all(kinds[j]=='JOINT_'+kinds[p] for j,p in zip(chain.joint_id,chain.primary_id)))
    check('prescreen three directions',len(px)==len(ps)*len(ENUMS[PX,'ext_ref_direction_cd']) and (px.groupby('ps_prescreen_entry_id').size()==3).all())
    check('prescreen temporal windows',((ps.effective_start_d<=ps.first_look_up)&(ps.first_look_up<=ps.last_look_up)&(ps.last_look_up<=ps.effective_end_d)).all())
    check('prescreen used null semantics',(ps.used_d.notna()==ps.status_cd.eq('USED')).all())
    check('prescreen used within window',((ps.loc[ps.used_d.notna(),'used_d']>=ps.loc[ps.used_d.notna(),'first_look_up']) & (ps.loc[ps.used_d.notna(),'used_d']<=ps.loc[ps.used_d.notna(),'effective_end_d'])).all())
    uw=tables[U]
    check('UW decision null semantics',uw.decision_status_d.notna().equals(uw.decision_status_cd.notna()))
    check('UW status/expiry order',((uw.status_d>=uw.created_d)&(uw.expiration_d>=uw.status_d)&(uw.modified_d>=uw.status_d)).all())
    check('UW decision temporal',(uw.loc[uw.decision_status_d.notna(),'decision_status_d'].between(uw.loc[uw.decision_status_d.notna(),'created_d'],uw.loc[uw.decision_status_d.notna(),'modified_d'])).all())
    CHECK_TOTAL+=len(checks)
    print('VALIDATION',label,'PASSED',len(checks),'asserted checks; predicates:', '; '.join(checks))
    return True

def read_table(source,t):
    types={c:'string' for c,typ in SCHEMA[t].items() if not typ.startswith('timestamp') and not typ.startswith('decimal')}
    for c,typ in SCHEMA[t].items():
        if typ in ('bigint','integer','smallint','tinyint'): types[c]='int64'
    converters={c:lambda v: Decimal(v).quantize(MONEY_QUANTUM) for c,typ in SCHEMA[t].items() if typ.startswith('decimal')}
    df=pd.read_csv(source,dtype=types,converters=converters,keep_default_na=False,na_values=[''])
    if '_operation' in df:
        assert not df._operation.isin(['UPDATE','DELETE']).any(), 'CDC updates/deletes need a merged baseline, not a delta snapshot'
        df=df.drop(columns='_operation')
    for c,typ in SCHEMA[t].items():
        if typ.startswith('timestamp'): df[c]=pd.to_datetime(df[c])
    return df

def table_key(t):
    schema,table=t.split('.')
    return '/'.join([S3_PREFIX.strip('/'),schema,table+'.'+OUTPUT_FORMAT])

def load_existing(bucket,prefix,local_dir=None):
    assert prefix==S3_PREFIX and OUTPUT_FORMAT=='csv'
    result={}; client=None if local_dir else boto3.client('s3')
    for t in ORDER:
        source=Path(local_dir)/(t+'.csv') if local_dir else io.BytesIO(client.get_object(Bucket=bucket,Key=table_key(t))['Body'].read())
        result[t]=read_table(source,t)
    return result

def generate_incremental(existing,rng,fake):
    alloc=Allocator(existing); chain=primary_chain(existing)
    partner=dict(zip(chain.joint_id,chain.primary_id)); partner.update(dict(zip(chain.primary_id,chain.joint_id)))
    desired=int(round(len(existing[A])*DELETE_PCT/100)); delete_apps=set()
    for key in rng.permutation(existing[A].id.to_numpy()):
        if len(delete_apps)>=desired: break
        delete_apps.add(key)
        if key in partner: delete_apps.add(partner[key])
    removed={t:set() for t in SCHEMA}; removed[A]=delete_apps
    removed[P]=set(existing[P].loc[existing[P].application_id.isin(delete_apps),'id'])
    removed[PS]=set(chain.loc[chain.joint_id.isin(delete_apps),'ps_id'])
    removed[O]=set(existing[X].loc[(existing[X].ext_entity_name=='applicantid') & existing[X].ext_ref_id.isin(removed[P]),'origination_id'])
    for child,fk,parent in FKS:
        removed[child]|=set(existing[child].loc[existing[child][fk].isin(removed[parent]),'id'])
    rx=existing[RX]
    semantic_deleted=((rx.ext_ref_entity_name=='applicantid') & rx.ext_ref_id.isin(removed[P])) | ((rx.ext_ref_entity_name=='prescreenentry') & rx.ext_ref_id.isin(removed[PS]))
    protected_refs=rx[semantic_deleted & ~rx.id.isin(removed[RX])]
    assert protected_refs.empty, 'A surviving underwriting run depends on an entity selected for deletion'
    merged={}; delta={t:[] for t in SCHEMA}
    for t in reversed(ORDER):
        df=existing[t]; deleted=df[df.id.isin(removed[t])].copy(); deleted['_operation']='DELETE'; delta[t].append(deleted)
        merged[t]=df[~df.id.isin(removed[t])].copy()
    for t in ORDER:
        count=min(len(merged[t]),int(round(len(existing[t])*UPDATE_PCT/100)))
        idx=rng.choice(merged[t].index.to_numpy(),size=count,replace=False)
        merged[t].loc[idx,'version']+=1
        merged[t].loc[idx,'modified_d']+=pd.Timedelta(seconds=UW_STATUS_OFFSET_SECONDS)
        changed=merged[t].loc[idx].copy(); changed['_operation']='UPDATE'; delta[t].append(changed)
    insert_count=int(round(len(existing[A])*INSERT_PCT/100))
    added=generate_full(rng,fake,alloc,insert_count) if insert_count else {t:df.iloc[:0].copy() for t,df in existing.items()}
    for t in ORDER:
        assert set(added[t].id).isdisjoint(existing[t].id)
        merged[t]=pd.concat([merged[t],added[t]],ignore_index=True)
        ins=added[t].copy(); ins['_operation']='INSERT'; delta[t].append(ins)
        delta[t]=pd.concat(delta[t],ignore_index=True)
    return merged,delta

def incremental_self_test(baseline):
    rng,fake=seed_sources()
    with tempfile.TemporaryDirectory() as directory:
        for t,df in baseline.items(): df.to_csv(Path(directory)/(t+'.csv'),index=False)
        loaded=load_existing(S3_BUCKET,S3_PREFIX,local_dir=directory)
    validate(loaded,'CSV round-trip',expected_root=len(baseline[A]),full_targets=True)
    merged,delta=generate_incremental(loaded,rng,fake)
    validate(merged,'incremental self-test')
    for t in ORDER:
        counts=delta[t]._operation.value_counts().to_dict(); print('CDC COUNTS',t,counts)
        for op,pct in [('INSERT',INSERT_PCT),('UPDATE',UPDATE_PCT),('DELETE',DELETE_PCT)]:
            expected=len(loaded[t])*pct/100
            assert abs(counts.get(op,0)-expected)<=max(OP_TOLERANCE_ROWS,expected*OP_TOLERANCE_FRACTION),(t,op,counts,expected)
        replay=loaded[t].set_index('id')
        changes=delta[t]; replay=replay.drop(changes.loc[changes._operation=='DELETE','id'])
        updates=changes[changes._operation=='UPDATE'].drop(columns='_operation').set_index('id'); replay.loc[updates.index,updates.columns]=updates
        replay=pd.concat([replay,changes[changes._operation=='INSERT'].drop(columns='_operation').set_index('id')]).reset_index()
        pd.testing.assert_frame_equal(replay.sort_values('id').reset_index(drop=True),merged[t].sort_values('id').reset_index(drop=True),check_dtype=False)
    assert not merged[A].equals(baseline[A]), 'incremental must not equal fresh full generation'
    print('SELF-TEST PASSED: real CSV parser, CDC replay, operation rates, monotonic IDs and all business rules')

def upload(tables):
    client=boto3.client('s3'); first=table_key(ORDER[0]); print('First S3 key:',first)
    assert '//' not in first.replace('s3://','') and not first.startswith('/')
    for t in ORDER:
        key=table_key(t); assert '//' not in key
        client.put_object(Bucket=S3_BUCKET,Key=key,Body=tables[t].to_csv(index=False).encode('utf-8'),ContentType='text/csv')
        print('Uploaded s3://'+S3_BUCKET+'/'+key)

def main():
    assert OUTPUT_FORMAT=='csv' and MODE in ('full','incremental') and INCREMENTAL_OUTPUT in ('merged','delta')
    assert all(0<=v<=100 for v in (INSERT_PCT,UPDATE_PCT,DELETE_PCT))
    print('Mode',MODE,'Seed',SEED,'Reference label exclusions:',EXCLUSIONS)
    rng,fake=seed_sources(); baseline=generate_full(rng,fake)
    validate(baseline,'full',expected_root=round(ROOT_ROW_COUNTS[A]*SCALE_FACTOR),full_targets=True)
    incremental_self_test(baseline)
    if MODE=='full': tables=baseline
    else:
        existing=load_existing(S3_BUCKET,S3_PREFIX); validate(existing,'existing baseline')
        rng,fake=seed_sources(); merged,delta=generate_incremental(existing,rng,fake); validate(merged,'incremental production')
        tables=merged if INCREMENTAL_OUTPUT=='merged' else delta
    print('TOTAL ASSERTED VALIDATION CHECKS',CHECK_TOTAL)
    for t,df in tables.items(): print('TABLE',t,len(df),'rows',len(df.columns),'columns; golden=false')
    if UPLOAD_ENABLED: upload(tables)
    else: print('UPLOAD SKIPPED (dev/test run)')
    return tables

TABLES=main()