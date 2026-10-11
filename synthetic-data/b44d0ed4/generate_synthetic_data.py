#!/usr/bin/env python3
"""Streaming banking generator with non-destructive SQL naming compatibility.
Requires numpy, pandas, pyarrow, faker, duckdb; boto3 only for S3.
Examples:
 python revised_banking_generator.py --applications 100000000 --no-upload
 python revised_banking_generator.py --applications 1000 --no-upload --self-test
 python revised_banking_generator.py --mode incremental --existing-local-dir /data/base --local-dir /data/next --no-upload
 python revised_banking_generator.py --install-compatibility --local-dir /data/downloaded
CSV is the default. Physical files retain all 15 Table_info schemas (171 columns).
_compatibility.duckdb.sql and _compatibility.duckdb expose requirement-name views,
including source_code, without copying rows or changing physical CSV columns.
SQL files/databases contain local absolute paths; regenerate after relocation.
Counter-based RNG is reproducible but differs from legacy RNG sequencing.
"""
import argparse, copy, hashlib, json, math, os, sys
from pathlib import Path
from decimal import Decimal
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from faker import Faker

APP='lc20_as.as_application'; APPLICANT='lc20_as.as_applicant'
DETAIL='lc20_as.as_applicant_detail'; EMAIL='lc20_as.as_email_address'
PS='lc20_ps.ps_prescreen_entry'; PSREF='lc20_ps.ps_prescreen_ext_ref'
ORIGIN='lc20_das.das_origination'; MEMBER='lc20_das.das_member_origination'
ORIGINREF='lc20_das.das_origination_ext_ref'; UW='lc20_uw_processing.uw_process'
RUN='lc20_uw_processing.uw_process_run'; RUNREF='lc20_uw_processing.uw_process_run_ext_ref'
SAV='lc20_as.as_application_saving_account'; CHECK='lc20_as.as_application_checking_account'; CD='lc20_as.as_application_certificate_of_deposit_account'
ORDER=[APP,APPLICANT,SAV,CHECK,CD,DETAIL,EMAIL,PS,ORIGIN,MEMBER,ORIGINREF,PSREF,UW,RUN,RUNREF]
# Independent physical contract: s=string/varchar, t=timestamp(ms), b=bigint,
# f=tinyint, i=integer, h=smallint, d=decimal(20,2), o=unqualified decimal(38,2).
CONTRACT={
APP:'id:s created_d:t modified_d:t version:b application_d:t product_type_cd:s application_type_cd:s',
APPLICANT:'id:s created_d:t modified_d:t version:b application_id:s primary_applicant_flg:f sms_marketing_opt_in_flg:f marketing_attribution_cd:s',
SAV:'id:s created_d:t modified_d:t version:b source_cd:s application_id:s initial_deposit_amount:s product_code:s is_joint_flg:f debit_card_requested_flg:f account_type:s',
CHECK:'id:s created_d:t modified_d:t version:b application_id:s source_cd:s debit_card_requested_flg:f min_initial_deposit_amount:s initial_deposit_amount:s product_code:s is_joint_flg:f account_type:s',
CD:'id:s created_d:t modified_d:t version:b source_cd:s initial_deposit_amount:s product_code:s application_id:s apy:d term_in_months:i is_joint_flg:f account_type:s',
DETAIL:'id:s created_d:t modified_d:t version:b applicant_id:s source_cd:s first_name:s first_name_keyn:s first_name_keyv:i last_name:s last_name_keyn:s last_name_keyv:i honorific_cd:s date_of_birth:s date_of_birth_keyn:s date_of_birth_keyv:i ssn:s ssn_keyn:s ssn_keyv:i marital_status_cd:s educational_qualification_cd:s employment_status_cd:s cumulative_job_tenure_months:s annual_income:s annual_additional_income:s annual_income_taxable:s annual_income_non_taxable:s housing_status_cd:s annual_housing_expense:s annual_additional_debt:s fico_range_cd:s fico_min:s fico_max:s citizenship:f',
EMAIL:'id:s created_d:t modified_d:t version:b applicant_id:s source_cd:s email_address:s email_address_keyn:s email_address_keyv:i',
UW:'id:s created_d:t modified_d:t version:b subject_ref_id:s subject_entity_name:s type_cd:s subject_src_id:s status_cd:s product_type_cd:s status_d:t decision_status_cd:s decision_status_d:t decision_phase_cd:s subject_type_cd:s expiration_d:t',
RUN:'id:s created_d:t modified_d:t version:b uw_process_id:s decision_phase_cd:s decision_status_cd:s',
RUNREF:'id:s created_d:t modified_d:t version:b uw_process_run_id:s ext_ref_id:s ext_src_id:s ext_ref_entity_name:s',
ORIGIN:'id:s created_d:t modified_d:t version:b origination_guid:s',
MEMBER:'id:s created_d:t modified_d:t version:b origination_id:s member_ref_id:s member_src_id:s member_entity_name:s',
ORIGINREF:'id:s created_d:t modified_d:t version:b origination_id:s ext_ref_id:s ext_src_id:s ext_entity_name:s flow_id:s flow_version:h',
PS:'id:s created_d:t modified_d:t version:b source_cd:s credit_policy_name:s credit_policy_version:h effective_start_d:t effective_end_d:t status_cd:s first_look_up:t last_look_up:t used_d:t offer_amt:o offer_purpose_cd:s',
PSREF:'id:s created_d:t modified_d:t version:b ps_prescreen_entry_id:s ext_src_id:s ext_ref_id:s ext_entity_name:s ext_ref_direction_cd:s'}
TYPES={'s':pa.string(),'t':pa.timestamp('ms'),'b':pa.int64(),'f':pa.int8(),'i':pa.int32(),'h':pa.int16(),'d':pa.decimal128(20,2),'o':pa.decimal128(38,2)}
SQLT={'s':'string','t':'timestamp(3)','b':'bigint','f':'tinyint','i':'integer','h':'smallint','d':'decimal(20,2)','o':'decimal'}
DBT={'s':'VARCHAR','t':'TIMESTAMP_MS','b':'BIGINT','f':'TINYINT','i':'INTEGER','h':'SMALLINT','d':'DECIMAL(20,2)','o':'DECIMAL(38,2)'}
SCHEMAS={t:pa.schema([pa.field(x.split(':')[0],TYPES[x.split(':')[1]]) for x in spec.split()]) for t,spec in CONTRACT.items()}
ALWAYS_NULL={APP:['product_type_cd'],APPLICANT:['marketing_attribution_cd'],DETAIL:['honorific_cd','marital_status_cd','educational_qualification_cd','fico_range_cd'],UW:['product_type_cd']}
NULLABLE=set(sum(ALWAYS_NULL.values(),[]))|{'application_type_cd','employment_status_cd','housing_status_cd','decision_status_cd','decision_phase_cd','decision_status_d','used_d'}
DATE_COLUMNS={'created_d','modified_d','application_d','status_d','decision_status_d','expiration_d','effective_start_d','effective_end_d','first_look_up','last_look_up','used_d'}
FLAGS={'primary_applicant_flg','sms_marketing_opt_in_flg','is_joint_flg','debit_card_requested_flg','citizenship'}
DISTRIBUTIONS={
'application_type':{'values':['SAVINGS_ACCOUNT','CHECKING_ACCOUNT','CERTIFICATE_DEPOSIT_ACCOUNT','JOINT_SAVINGS_ACCOUNT','JOINT_CHECKING_ACCOUNT','JOINT_CERTIFICATE_DEPOSIT_ACCOUNT',None],'weights':[.25,.25,.20,.10,.10,.08,.02]},
'employment':{'values':['UNEMPLOYED','EMPLOYED','RETIRED','SELFEMPLOYED','DISABLED','OTHER',None],'weights':[.06,.58,.14,.12,.02,.03,.05]},
'housing':{'values':['OWN','MORTGAGE','RENT','NONE','ANY','OTHER',None],'weights':[.20,.38,.30,.03,.01,.03,.05]},
'uw_status':{'values':['FAILURE','CANCELED','SUCCESS','EXPIRED','WITHDRAWN','ACTIVE','NEW'],'weights':[.08,.03,.65,.03,.02,.14,.05]},
'decision':{'values':[None,'FAILURE','ERROR','IN_PROGRESS','SUCCESS','MORE_INFO','PENDING'],'weights':[.10,.08,.02,.15,.50,.05,.10]},
'phase':{'values':['FULL_DECISION','ADDRESS_VALIDATION','BANK_ACCOUNT','FULFILLMENT',None],'weights':[.55,.15,.10,.15,.05]},
'prescreen_status':{'values':['ACTIVE','USED','EXPIRED'],'weights':[.50,.40,.10]},
'uw_ext_entity':{'values':['narmiuuid','bankaccount','prescreenentry','journeyapplicationtoken','applicantid','cif','loginactorid','entitytoken','offerid'],'weights':[1/9]*9}}
NULL_PCTS={'application_type':.02,'employment':.05,'housing':.05,'decision':.10,'phase':.05}
CARDINALITIES={'details':1.,'emails':1.2,'uw_processes':1.2,'uw_runs':1.5,'uw_run_refs':1.2,'members':1.}
MONEY={'saving_deposit':(500.,1.1,0.,100000.),'checking_deposit':(500.,1.,25.,100000.),'cd_deposit':(5000.,.9,500.,250000.),'income':(60000.,.65,1000.,1000000.),'additional_income':(4000.,.9,0.,100000.),'housing_expense':(18000.,.55,0.,150000.),'debt':(5000.,.9,0.,250000.),'offer':(10000.,.6,1000.,50000.)}
ACCOUNTS={SAV:([0,3],['09','10','25'],'SV','saving_deposit'),CHECK:([1,4],['53','01'],'ND','checking_deposit'),CD:([2,5],['14','26','25','22','06','12','21','23','28'],'CD','cd_deposit')}
PROFILE={APP:[('application_type_cd','application_type')],DETAIL:[('employment_status_cd','employment'),('housing_status_cd','housing')],UW:[('status_cd','uw_status'),('decision_status_cd','decision'),('decision_phase_cd','phase')],RUN:[('decision_status_cd','decision'),('decision_phase_cd','phase')],PS:[('status_cd','prescreen_status')]}
FKS=[(APPLICANT,'application_id',APP,None,None)]+[(t,'application_id',APP,None,None) for t in ACCOUNTS]+[(DETAIL,'applicant_id',APPLICANT,None,None),(EMAIL,'applicant_id',APPLICANT,None,None),(MEMBER,'origination_id',ORIGIN,None,None),(ORIGINREF,'origination_id',ORIGIN,None,None),(ORIGINREF,'ext_ref_id',APP,'ext_entity_name','applicationid'),(ORIGINREF,'ext_ref_id',APPLICANT,'ext_entity_name','applicantid'),(ORIGINREF,'ext_ref_id',PS,'ext_entity_name','prescreenentry'),(PSREF,'ps_prescreen_entry_id',PS,None,None),(PSREF,'ext_ref_id',APP,'ext_entity_name','applicationid'),(UW,'subject_ref_id',APP,'subject_entity_name','applicationid'),(RUN,'uw_process_id',UW,None,None),(RUNREF,'uw_process_run_id',RUN,None,None),(RUNREF,'ext_ref_id',APPLICANT,'ext_ref_entity_name','applicantid'),(RUNREF,'ext_ref_id',PS,'ext_ref_entity_name','prescreenentry')]
GROUPS={DETAIL:('applicant_id',APPLICANT),EMAIL:('applicant_id',APPLICANT),MEMBER:('origination_id',ORIGIN),UW:('subject_ref_id',APP),RUN:('uw_process_id',UW),RUNREF:('uw_process_run_id',RUN)}
PROTECTED={APPLICANT,ORIGINREF,PSREF}|set(ACCOUNTS)
TEMPORAL={APP:[('created_d','application_d'),('application_d','modified_d')],PS:[('created_d','effective_start_d'),('effective_start_d','first_look_up'),('first_look_up','last_look_up'),('last_look_up','used_d'),('used_d','effective_end_d'),('last_look_up','effective_end_d')],UW:[('created_d','status_d'),('status_d','decision_status_d'),('decision_status_d','expiration_d'),('status_d','expiration_d')]}
TABLE_ALIASES={'lc20_as.as_application_savings_account':SAV,'lc20_as.ps_prescreen_entry':PS,'lc20_as.ps_prescreen_ext_ref':PSREF,'lc20_as.das_origination':ORIGIN,'lc20_as.das_origination_ext_ref':ORIGINREF,'lc20_as.das_member_origination':MEMBER}
COLUMN_ALIASES={DETAIL:{'source_code':'source_cd'}}
ASSUMPTIONS=['Table_info defines physical tables; conflicting relationship/enum names are supported by SQL compatibility views.','Applications are roots; one origination each, one prescreen per joint application.','Each joint has two applicants; otherwise one; exactly one primary.','Each joint maps to a same-product non-joint application via a SOURCE reference.','Statistics are configurable assumptions; no golden tables supplied.','No Values means null; offer_amt assumes decimal(38,2); timestamps are milliseconds.','External tokens are synthetic; SSNs use invalid 000 prefix; key metadata is not encryption.','Root inserts cascade; deletes stop at dependencies and mandatory coverage.','Counter-based RNG preserves sampling logic, not legacy random byte sequences.']
MS_DAY=86400000; MASK64=(1<<64)-1

def require(ok,msg):
    if not bool(ok):raise ValueError(msg)
def schema(t,cdc=False):return SCHEMAS[t].append(pa.field('_operation',pa.string(),nullable=False)) if cdc else SCHEMAS[t]
def q(x):return '"'+x.replace('"','""')+'"'
def lit(x):return "'"+str(x).replace("'","''")+"'"
def fq(t):return '.'.join(q(x) for x in t.split('.'))
def physical(t):s,n=t.split('.');return '_physical_'+s+'.'+n
def relpath(t,fmt):s,n=t.split('.');return Path(s)/(n+'.'+fmt)
def dbname(prefix,t):return prefix+str(ORDER.index(t))

def parse_config(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    for k,v,ty in [('seed',42,int),('applications',1000,int),('scale-factor',1.,float),('batch-size',20000,int),('max-batch-rows',100000,int),('threads',2,int),('insert-pct',10.,float),('update-pct',5.,float),('delete-pct',2.,float)]:p.add_argument('--'+k,type=ty,default=v)
    for k,v in [('bucket','mm-fsi-fix'),('prefix','lc/synthetic_data_1/runs/20261011-222304/'),('existing-prefix',None),('local-dir','/tmp/lc_synthetic_revised'),('existing-local-dir',None),('date-start','2025-01-01T00:00:00.000'),('date-end','2026-10-11T23:59:59.999'),('memory-limit','2GB')]:p.add_argument('--'+k,default=v)
    p.add_argument('--format',choices=['csv','parquet'],default='csv');p.add_argument('--input-format',choices=['csv','parquet'])
    p.add_argument('--mode',choices=['full','incremental'],default='full');p.add_argument('--incremental-output',choices=['merged','delta'],default='merged')
    for k in ['no-upload','self-test','keep-work','install-compatibility']:p.add_argument('--'+k,action='store_true')
    for k in ['cardinalities','distributions','null-pcts','money-profiles']:p.add_argument('--'+k+'-json',default='{}')
    c=vars(p.parse_args(argv));c['prefix']=c['prefix'].strip('/')+'/';c['existing_prefix']=(c['existing_prefix'] or c['prefix']).strip('/')+'/'
    c['input_format']=c['input_format'] or c['format']
    for k,default in [('cardinalities',CARDINALITIES),('distributions',DISTRIBUTIONS),('null_pcts',NULL_PCTS),('money_profiles',MONEY)]:
        o=json.loads(c.pop(k+'_json'));require(isinstance(o,dict) and not set(o)-set(default),'Unknown '+k+' configuration');c[k]=copy.deepcopy(default);c[k].update(o)
    require(c['applications']>=0 and math.isfinite(c['scale_factor']) and c['scale_factor']>=0,'Invalid root count/scale')
    require(c['batch_size']>0 and c['max_batch_rows']>0 and c['threads']>0,'Invalid batch/thread limits')
    for k in ['insert_pct','update_pct','delete_pct']:require(math.isfinite(c[k]) and 0<=c[k]<=100,'Invalid change percentage')
    for v in c['cardinalities'].values():require(math.isfinite(v) and v>=1,'Required child average must be >=1')
    for v in c['null_pcts'].values():require(math.isfinite(v) and 0<=v<=1,'Invalid null fraction')
    for k,s in c['distributions'].items():
        s['values']=[None if v is None or str(v).strip().lower()=='null' else str(v).strip() for v in s['values']];w=np.asarray(s['weights'],float)
        require(len(w)==len(s['values']) and len(set(s['values']))==len(w) and not set(s['values'])-set(DISTRIBUTIONS[k]['values']),'Invalid enum '+k)
        require(np.isfinite(w).all() and (w>=0).all() and w.sum()>0,'Invalid weights');w=w/w.sum()
        if None in s['values']:
            rate=c['null_pcts'][k];nn=np.array([v is not None for v in s['values']]);require(w[nn].sum()>0 or rate==1,'Missing non-null weight');w[nn]=w[nn]/max(w[nn].sum(),np.finfo(float).tiny)*(1-rate);w[~nn]=rate
        s['weights']=w.tolist()
    for k,x in c['money_profiles'].items():
        require(len(x)==4 and all(math.isfinite(v) for v in x) and x[0]>0 and x[1]>0 and x[2]>=0 and x[3]>x[2],'Invalid money profile '+k)
        require(math.ceil(x[2]*100)<=math.floor(x[3]*100),'No representable cents')
    require(c['money_profiles']['checking_deposit'][2]>=25,'Checking minimum must be >=25')
    for k in ['date_start','date_end']:
        d=pd.Timestamp(c[k]);require(d.tz is None and d.value%1000000==0,'Timezone-naive millisecond dates required');c[k+'_ms']=d.value//1000000
    require(c['date_end_ms']-c['date_start_ms']>60*MS_DAY,'Date window must exceed 60 days')
    if c['mode']=='incremental' and c['prefix']==c['existing_prefix'] and c['incremental_output']=='delta':c['prefix']+='delta/'
    require(not c['self_test'] or (c['mode']=='full' and int(round(c['applications']*c['scale_factor']))==1000 and c['no_upload']),'Self-tests require full mode, 1000 applications and --no-upload')
    pa.set_cpu_count(c['threads']);pa.set_io_thread_count(c['threads']);return c

def mix(x):
    x=np.asarray(x,dtype=np.uint64).copy()
    with np.errstate(over='ignore'):
        x=(x^(x>>np.uint64(30)))*np.uint64(0xbf58476d1ce4e5b9);x=(x^(x>>np.uint64(27)))*np.uint64(0x94d049bb133111eb)
    return x^(x>>np.uint64(31))
def salt(seed,label):return int.from_bytes(hashlib.blake2b((str(seed)+':'+label).encode(),digest_size=8).digest(),'little')
def permute(x,n,key,inverse=False):
    x=np.asarray(x,dtype=np.uint64).copy()
    if n<=1:return x.astype(np.int64)
    half=((int(n)-1).bit_length()+1)//2;mask=np.uint64((1<<half)-1);shift=np.uint64(half)
    def step(a):
        l=a>>shift;r=a&mask
        for k in (range(3,-1,-1) if inverse else range(4)):
            h=np.uint64((int(key)+k*0x9e3779b9)&MASK64)
            if inverse:l,r=(r^(mix(l^h)&mask)),l
            else:l,r=r,(l^(mix(r^h)&mask))
        return (l<<shift)|r
    x=step(x);bad=x>=n
    while bad.any():x[bad]=step(x[bad]);bad=x>=n
    return x.astype(np.int64)
class Random:
    def __init__(self,seed):self.seed=seed
    def u(self,ids,label,attempt=0):
        bits=mix(np.asarray(ids,np.uint64)^np.uint64(salt(self.seed,label+':'+str(attempt))));return ((bits>>np.uint64(11)).astype(np.float64)+.5)/(1<<53)
    def integer(self,ids,label,low,high):return low+np.floor(self.u(ids,label)*(np.asarray(high)-low)).astype(np.int64)
    def choice(self,ids,label,values,weights=None):
        weights=np.ones(len(values))/len(values) if weights is None else np.asarray(weights)/sum(weights);j=np.searchsorted(np.cumsum(weights),self.u(ids,label),side='right');return np.asarray(values,dtype=object)[np.minimum(j,len(values)-1)]
    def profile(self,ids,name,c,stream=''):s=c['distributions'][name];return self.choice(ids,stream+name,s['values'],s['weights'])
    def money(self,ids,name,c):
        median,sigma,lo,hi=c['money_profiles'][name];out=np.empty(len(ids),np.int64);pending=np.arange(len(ids))
        for attempt in range(10000):
            if not len(pending):return out
            u1=self.u(ids[pending],name+'normal1',attempt);u2=self.u(ids[pending],name+'normal2',attempt);z=np.sqrt(-2*np.log(u1))*np.cos(2*np.pi*u2)
            with np.errstate(over='ignore'):v=np.rint(np.exp(np.log(median)+sigma*z)*100)
            good=np.isfinite(v)&(v>=math.ceil(lo*100))&(v<=math.floor(hi*100))&(v<=np.iinfo(np.int64).max);out[pending[good]]=v[good].astype(np.int64);pending=pending[~good]
        raise ValueError('Truncated lognormal rejection did not converge: '+name)
def strings(x):return np.asarray(x).astype(str)
def amounts(x):
    x=np.asarray(x,np.int64);return np.char.add(np.char.add(strings(x//100),'.'),np.char.zfill(strings(x%100),2))
def dec(x,typ):return pc.cast(pa.array(amounts(x)),typ,safe=True)
def uuids(ids,r,label):
    a=mix(np.asarray(ids,np.uint64)^np.uint64(salt(r.seed,label+'a')));b=mix(np.asarray(ids,np.uint64)^np.uint64(salt(r.seed,label+'b')))
    data=np.column_stack([a,b]).astype('<u8').view(np.uint8).reshape(-1,16);data[:,6]=(data[:,6]&15)|64;data[:,8]=(data[:,8]&63)|128
    text=pa.array(np.frombuffer(data.tobytes().hex().encode(),dtype='S32').astype(str))
    return pc.binary_join_element_wise(pc.utf8_slice_codeunits(text,0,8),pc.utf8_slice_codeunits(text,8,12),pc.utf8_slice_codeunits(text,12,16),pc.utf8_slice_codeunits(text,16,20),pc.utf8_slice_codeunits(text,20,32),pa.scalar('-'))
def base(t,ids,c,r,parent=None):
    ids=np.asarray(ids,np.int64);created=r.integer(ids,t+'root',c['date_start_ms'],c['date_end_ms']-60*MS_DAY+1) if parent is None else np.asarray(parent,np.int64)+r.integer(ids,t+'link',0,12*3600000+1)
    return {'id':strings(ids),'created_d':created,'modified_d':created+r.integer(ids,t+'modified',0,24*3600000+1),'version':np.ones(len(ids),np.int64)}
def table(t,d,cdc=False):
    n=len(d['id']);out=[]
    for f in schema(t,cdc):
        v=d.get(f.name)
        if v is None:out.append(pa.nulls(n,type=f.type))
        elif isinstance(v,(pa.Array,pa.ChunkedArray)):out.append(pc.cast(v,f.type,safe=True))
        elif np.isscalar(v):out.append(pa.array([v]*n,type=f.type))
        elif pa.types.is_timestamp(f.type):out.append(pa.array(np.asarray(v,dtype='datetime64[ms]'),type=f.type))
        else:out.append(pa.array(v,type=f.type))
    a=pa.Table.from_arrays(out,schema=schema(t,cdc));validate_table(a,t,cdc);return a
def numpy_col(t,c):return t[c].to_numpy(zero_copy_only=False)
def numeric(t,c):return numpy_col(t,c).astype(np.int64)
def all_pc(x):return bool(pc.all(pc.fill_null(x,True)).as_py()) if len(x) else True

def validate_table(a,t,cdc=False,cfg=None):
    require(a.schema.remove_metadata().equals(schema(t,cdc),check_metadata=False),'Exact schema '+t);n=len(a)
    if not n:return
    require(len(pc.unique(a['id']))==n and all_pc(pc.match_substring_regex(a['id'],r'^[1-9][0-9]*$')),'PK uniqueness/pattern '+t)
    for f in a.schema:
        if f.name not in NULLABLE:require(a[f.name].null_count==0,'Required '+t+'.'+f.name)
        if f.name in FLAGS:require(all_pc(pc.is_in(a[f.name],value_set=pa.array([0,1],type=pa.int8()))),'Flag '+f.name)
        if cfg and cfg['mode']=='full' and f.name in DATE_COLUMNS:
            v=pc.cast(a[f.name],pa.int64());require(all_pc(pc.and_(pc.greater_equal(v,cfg['date_start_ms']),pc.less_equal(v,cfg['date_end_ms']))),'Date bounds '+t+'.'+f.name)
    require(all_pc(pc.greater_equal(a['version'],1)) and all_pc(pc.greater_equal(a['modified_d'],a['created_d'])),'Version/modified '+t)
    for col in ALWAYS_NULL.get(t,[]):require(a[col].null_count==n,'Always null '+col)
    for before,after in TEMPORAL.get(t,[]):require(all_pc(pc.less_equal(a[before],a[after])),'Temporal '+t+'.'+after)
    for col,p in PROFILE.get(t,[]):require(all_pc(pc.is_in(pc.drop_null(a[col]),value_set=pa.array([v for v in DISTRIBUTIONS[p]['values'] if v is not None]))),'Enum '+col)
    if t in ACCOUNTS:
        _,codes,typ,m=ACCOUNTS[t]
        require(all_pc(pc.equal(a['source_cd'],'APPLICANT')) and all_pc(pc.equal(a['account_type'],typ)) and all_pc(pc.is_in(a['product_code'],value_set=pa.array(codes))),'Account enums')
        require(all_pc(pc.match_substring_regex(a['initial_deposit_amount'],r'^\d+\.\d{2}$')),'Deposit format');money=pc.cast(a['initial_deposit_amount'],pa.decimal128(20,2))
        if t==CHECK:require(all_pc(pc.greater_equal(money,Decimal('25.00'))) and all_pc(pc.equal(a['min_initial_deposit_amount'],'25.00')),'Checking minimum')
        if cfg:
            lo,hi=cfg['money_profiles'][m][2:];require(all_pc(pc.and_(pc.greater_equal(money,Decimal(str(lo))),pc.less_equal(money,Decimal(str(hi))))),'Deposit bounds')
        if t==CD:require(all_pc(pc.and_(pc.greater_equal(a['apy'],Decimal('2.00')),pc.less_equal(a['apy'],Decimal('5.50')))) and all_pc(pc.is_in(a['term_in_months'],value_set=pa.array([6,12,18,24,36,60],type=pa.int32()))),'CD bounds')
    if t in [DETAIL,EMAIL,PS]:require(all_pc(pc.equal(a['source_cd'],'APPLICANT')),'Source')
    if t==DETAIL:
        require(all_pc(pc.match_substring_regex(a['ssn'],r'^000-\d{2}-\d{4}$')),'Safe SSN');dob=numpy_col(a,'date_of_birth').astype('datetime64[D]');dates=numpy_col(a,'created_d').astype('datetime64[D]')
        months=dates.astype('datetime64[M]');day=(dates-months.astype('datetime64[D]')).astype(np.int64);am=months-np.timedelta64(216,'M');md=((am+np.timedelta64(1,'M')).astype('datetime64[D]')-am.astype('datetime64[D]')).astype(np.int64)
        require((dob<=am.astype('datetime64[D]')+np.minimum(day,md-1).astype('timedelta64[D]')).all(),'Adult')
        lo=pc.cast(a['fico_min'],pa.int32()).to_numpy();hi=pc.cast(a['fico_max'],pa.int32()).to_numpy();require(np.isin(lo*1000+hi,[300579,580669,670739,740799,800850]).all(),'FICO')
        fields=['annual_income','annual_additional_income','annual_income_taxable','annual_income_non_taxable','annual_housing_expense','annual_additional_debt']
        for col in fields:require(all_pc(pc.match_substring_regex(a[col],r'^\d+\.\d{2}$')),'Money '+col)
        v=[pc.cast(a[col],pa.decimal128(20,2)) for col in fields[:4]];require(all_pc(pc.equal(pc.add(v[0],v[1]),pc.add(v[2],v[3]))),'Income accounting')
        for col in ['first_name','last_name','date_of_birth','ssn']:require(all_pc(pc.equal(a[col+'_keyn'],'SYNTHETIC_ONLY')) and all_pc(pc.equal(a[col+'_keyv'],1)),'Key metadata')
    if t==EMAIL:require(all_pc(pc.match_substring_regex(a['email_address'],r'^[^@\s]+@example\.com$')) and len(pc.unique(a['email_address']))==n,'Email')
    if t==MEMBER:
        s=numpy_col(a,'member_src_id');e=numpy_col(a,'member_entity_name');require(((s=='1000')&(e=='loginactorid')|(s=='1200')&(e=='pfloginuserid')).all(),'Member mapping')
    if t==ORIGINREF:
        ent=numpy_col(a,'ext_entity_name');src=numpy_col(a,'ext_src_id');require(np.isin(ent,['applicationid','applicantid','prescreenentry']).all() and np.where(ent=='prescreenentry',src=='11000',src=='11004').all(),'Origination ref enums/sources')
    if t==PSREF:
        for col,val in [('ext_ref_direction_cd','SOURCE'),('ext_src_id','11004'),('ext_entity_name','applicationid')]:require(all_pc(pc.equal(a[col],val)),'Prescreen ref '+col)
    if t==PS:require(all_pc(pc.equal(pc.is_valid(a['used_d']),pc.equal(a['status_cd'],'USED'))) and all_pc(pc.greater_equal(a['offer_amt'],Decimal('0.00'))),'Prescreen state')
    if t==UW:
        require(all_pc(pc.equal(pc.is_null(a['decision_status_d']),pc.is_null(a['decision_status_cd']))),'Decision null coupling')
        for col,val in [('subject_entity_name','applicationid'),('type_cd','APPLICATION_DECISION'),('subject_src_id','11004'),('subject_type_cd','APPLICATION')]:require(all_pc(pc.equal(a[col],val)),'UW '+col)
    if t==RUNREF:require(all_pc(pc.is_in(a['ext_ref_entity_name'],value_set=pa.array(DISTRIBUTIONS['uw_ext_entity']['values']))),'External enum')
    if cdc:require(all_pc(pc.is_in(a['_operation'],value_set=pa.array(['INSERT','UPDATE','DELETE']))),'CDC operations')

class Children:
    def __init__(self,n,avg,seed,label):self.n=n;self.target=int(round(n*avg));self.q,self.extra=divmod(self.target,n) if n else (0,0);self.key=salt(seed,label)
    def expand(self,o):
        o=np.asarray(o,np.int64);pos=np.repeat(np.arange(len(o)),self.q);ids=np.repeat(o,self.q)*self.q+np.tile(np.arange(self.q),len(o));rank=permute(o,self.n,self.key);extra=rank<self.extra
        return np.r_[pos,np.flatnonzero(extra)],np.r_[ids,self.q*self.n+rank[extra]]
class Plan:
    def __init__(self,c,n,starts=None):
        self.c=c;self.n=n;self.starts=starts or {t:1 for t in ORDER};self.r=Random(c['seed']);self.key=salt(c['seed'],'application permutation')
        s=c['distributions']['application_type'];w=np.array([dict(zip(s['values'],s['weights'])).get(v,0.) for v in DISTRIBUTIONS['application_type']['values']]);raw=w*n;counts=np.floor(raw).astype(np.int64);counts[np.argsort(-(raw-counts),kind='stable')[:n-int(counts.sum())]]+=1
        self.counts=counts;self.ends=np.cumsum(counts);self.beg=np.r_[0,self.ends[:-1]];self.j=int(counts[3:6].sum());self.total={APP:n,APPLICANT:n+self.j,ORIGIN:n,PS:self.j,PSREF:self.j,ORIGINREF:2*n+2*self.j}
        for t,(types,_,_,_) in ACCOUNTS.items():self.total[t]=int(counts[types].sum())
        self.children={}
        for t,parent,k in [(DETAIL,APPLICANT,'details'),(EMAIL,APPLICANT,'emails'),(MEMBER,ORIGIN,'members'),(UW,APP,'uw_processes'),(RUN,UW,'uw_runs'),(RUNREF,RUN,'uw_run_refs')]:
            p=Children(self.total[parent],c['cardinalities'][k],c['seed'],t+' cardinality');self.children[t]=p;self.total[t]=p.target
        a=c['cardinalities'];factor=max(4,2*math.ceil(a['details']),2*math.ceil(a['emails']),math.ceil(a['members']),math.ceil(a['uw_processes'])*math.ceil(a['uw_runs'])*math.ceil(a['uw_run_refs']))
        require(factor<=c['max_batch_rows'],'Single root exceeds --max-batch-rows');self.batch=min(c['batch_size'],max(1,c['max_batch_rows']//factor))
        provider=next(p for p in Faker('en_US').get_providers() if hasattr(p,'first_names'));self.names=[]
        for attr in ['first_names','last_names']:
            d=getattr(provider,attr);self.names.append((list(d),list(d.values()) if isinstance(d,dict) else None))
    def ids(self,t,o):return np.asarray(o,np.int64)+self.starts[t]
    def ranks(self,o):return permute(o,self.n,self.key)
    def created(self,o):return self.r.integer(self.ids(APP,o),APP+'root',self.c['date_start_ms'],self.c['date_end_ms']-60*MS_DAY+1)
    def joint_rank(self,o):return self.ranks(o)-self.beg[3]
    def batch_data(self,start,stop,existing=None):
        c=self.c;r=self.r;ao=np.arange(start,stop,dtype=np.int64);ai=self.ids(APP,ao);typ=np.searchsorted(self.ends,self.ranks(ao),side='right');joint=(typ>=3)&(typ<=5);jr=self.joint_rank(ao[joint]);result={}
        app=base(APP,ai,c,r);app['application_d']=app['created_d'];app['application_type_cd']=np.asarray(DISTRIBUTIONS['application_type']['values'],object)[typ];result[APP]=table(APP,app)
        ax=np.r_[np.arange(len(ao)),np.flatnonzero(joint)];apo=np.r_[ao,self.n+jr];apid=self.ids(APPLICANT,apo);ap=base(APPLICANT,apid,c,r,app['created_d'][ax]);ap.update(application_id=strings(ai[ax]),primary_applicant_flg=np.r_[np.ones(len(ao),np.int8),np.zeros(len(jr),np.int8)],sms_marketing_opt_in_flg=(r.u(apid,'sms')<.60).astype(np.int8));result[APPLICANT]=table(APPLICANT,ap)
        for t,(types,codes,account,money) in ACCOUNTS.items():
            ix=np.flatnonzero(np.isin(typ,types));rank=self.ranks(ao[ix]);o=np.where(typ[ix]==types[0],rank-self.beg[types[0]],self.counts[types[0]]+rank-self.beg[types[1]]);ids=self.ids(t,o);d=base(t,ids,c,r,app['created_d'][ix]);d.update(application_id=strings(ai[ix]),source_cd='APPLICANT',initial_deposit_amount=amounts(r.money(ids,money,c)),product_code=r.choice(ids,t+'code',codes),is_joint_flg=joint[ix].astype(np.int8),account_type=account)
            if t in [SAV,CHECK]:d['debit_card_requested_flg']=(r.u(ids,t+'debit')<.75).astype(np.int8)
            if t==CHECK:d['min_initial_deposit_amount']='25.00'
            if t==CD:d['apy']=dec(r.integer(ids,'apy',200,551),TYPES['d']);d['term_in_months']=r.choice(ids,'term',[6,12,18,24,36,60]).astype(np.int32)
            result[t]=table(t,d)
        ix,do=self.children[DETAIL].expand(apo);ids=self.ids(DETAIL,do);d=base(DETAIL,ids,c,r,ap['created_d'][ix]);d.update(applicant_id=strings(apid[ix]),source_cd='APPLICANT',first_name=r.choice(ids,'first name',*self.names[0]),last_name=r.choice(ids,'last name',*self.names[1]))
        age=r.integer(ids,'age',18,86);dates=d['created_d'].astype('datetime64[ms]').astype('datetime64[D]');months=dates.astype('datetime64[M]');day=(dates-months.astype('datetime64[D]')).astype(np.int64);bm=months-(age*12).astype('timedelta64[M]');md=((bm+np.timedelta64(1,'M')).astype('datetime64[D]')-bm.astype('datetime64[D]')).astype(np.int64)
        birth=bm.astype('datetime64[D]')+np.minimum(day,md-1).astype('timedelta64[D]')-r.integer(ids,'birth offset',0,365).astype('timedelta64[D]');d['date_of_birth']=strings(birth);d['ssn']=np.char.add(np.char.add(np.char.add('000-',np.char.zfill(strings(r.integer(ids,'ssn group',0,100)),2)),'-'),np.char.zfill(strings(r.integer(ids,'ssn serial',0,10000)),4))
        for f in ['first_name','last_name','date_of_birth','ssn']:d[f+'_keyn']='SYNTHETIC_ONLY';d[f+'_keyv']=1
        emp=r.profile(ids,'employment',c);d['employment_status_cd']=emp;d['housing_status_cd']=r.profile(ids,'housing',c);d['cumulative_job_tenure_months']=strings(r.integer(ids,'tenure',0,(age-18)*12+1));mult=np.ones(len(ids));mult[emp=='UNEMPLOYED']=.2;mult[emp=='DISABLED']=.35;mult[emp=='RETIRED']=.65
        income=np.rint(r.money(ids,'income',c)*mult).astype(np.int64);add=r.money(ids,'additional_income',c);tax=add*3//4
        for col,v in [('annual_income',income),('annual_additional_income',add),('annual_income_taxable',income+tax),('annual_income_non_taxable',add-tax),('annual_housing_expense',r.money(ids,'housing_expense',c)),('annual_additional_debt',r.money(ids,'debt',c))]:d[col]=amounts(v)
        band=r.integer(ids,'fico',0,5);d['fico_min']=np.array(['300','580','670','740','800'])[band];d['fico_max']=np.array(['579','669','739','799','850'])[band];d['citizenship']=(r.u(ids,'citizenship')<.95).astype(np.int8);result[DETAIL]=table(DETAIL,d)
        ix,eo=self.children[EMAIL].expand(apo);ids=self.ids(EMAIL,eo);d=base(EMAIL,ids,c,r,ap['created_d'][ix]);d.update(applicant_id=strings(apid[ix]),source_cd='APPLICANT',email_address=np.char.add(np.char.add(np.char.add(np.char.add('applicant',strings(apid[ix])),'.email'),strings(ids)),'@example.com'),email_address_keyn='SYNTHETIC_ONLY',email_address_keyv=1);result[EMAIL]=table(EMAIL,d)
        psids=self.ids(PS,jr);jt=typ[joint]-3;primary=np.empty(len(jr),np.int64);primary_dates=np.empty(len(jr),np.int64)
        for k in range(3):
            mask=jt==k;total=int(self.counts[k])+(existing.primary_counts[k] if existing else 0);require(total>0 or not mask.any(),'No same-product non-joint primary')
            if not mask.any():continue
            pick=r.integer(psids[mask],'primary '+str(k),0,total);old=pick<(existing.primary_counts[k] if existing else 0);mi=np.flatnonzero(mask)
            if old.any():primary[mi[old]],primary_dates[mi[old]]=existing.primary(k,pick[old])
            rank=pick[~old]-(existing.primary_counts[k] if existing else 0)+self.beg[k];parents=permute(rank,self.n,self.key,True);primary[mi[~old]]=self.ids(APP,parents);primary_dates[mi[~old]]=self.created(parents)
        ps=base(PS,psids,c,r,np.maximum(app['created_d'][joint],primary_dates));ps.update(source_cd='APPLICANT',credit_policy_name='SYNTHETIC_DEPOSIT_POLICY',credit_policy_version=1,effective_start_d=ps['created_d'],effective_end_d=ps['created_d']+30*MS_DAY,first_look_up=ps['created_d'],status_cd=r.profile(psids,'prescreen_status',c),offer_amt=dec(r.money(psids,'offer',c),TYPES['o']),offer_purpose_cd='DEPOSIT_ACCOUNT');ps['last_look_up']=ps['created_d']+r.integer(psids,'last lookup',0,12*3600000+1);ps['used_d']=pa.array(ps['last_look_up'].astype('datetime64[ms]'),mask=ps['status_cd']!='USED',type=TYPES['t']);ps['modified_d']=np.maximum(ps['modified_d'],ps['last_look_up']);result[PS]=table(PS,ps)
        oid=self.ids(ORIGIN,ao);origin=base(ORIGIN,oid,c,r,app['created_d']);origin['origination_guid']=uuids(oid,r,'origin guid');result[ORIGIN]=table(ORIGIN,origin)
        ix,mo=self.children[MEMBER].expand(ao);ids=self.ids(MEMBER,mo);d=base(MEMBER,ids,c,r,origin['created_d'][ix]);src=r.choice(ids,'member source',['1000','1200']);d.update(origination_id=strings(oid[ix]),member_ref_id=pc.binary_join_element_wise(pa.scalar('synthetic:'),uuids(ids,r,'member guid'),pa.scalar('')),member_src_id=src,member_entity_name=np.where(src=='1000','loginactorid','pfloginuserid'));result[MEMBER]=table(MEMBER,d)
        reford=np.r_[ao,self.n+apo,self.n+self.total[APPLICANT]+jr];ids=self.ids(ORIGINREF,reford);ox=np.r_[np.arange(len(ao)),ax,np.flatnonzero(joint)];d=base(ORIGINREF,ids,c,r,origin['created_d'][ox]);d.update(origination_id=strings(oid[ox]),ext_ref_id=np.r_[strings(ai),strings(apid),strings(psids)],ext_src_id=np.r_[np.full(len(ai)+len(apid),'11004'),np.full(len(jr),'11000')],ext_entity_name=np.r_[np.full(len(ai),'applicationid'),np.full(len(apid),'applicantid'),np.full(len(jr),'prescreenentry')],flow_id='synthetic-deposit-origination',flow_version=1);result[ORIGINREF]=table(ORIGINREF,d)
        ids=self.ids(PSREF,jr);d=base(PSREF,ids,c,r,ps['created_d']);d.update(ps_prescreen_entry_id=strings(psids),ext_src_id='11004',ext_ref_id=strings(primary),ext_entity_name='applicationid',ext_ref_direction_cd='SOURCE');result[PSREF]=table(PSREF,d)
        ix,uo=self.children[UW].expand(ao);uid=self.ids(UW,uo);u=base(UW,uid,c,r,app['created_d'][ix]);u.update(subject_ref_id=strings(ai[ix]),subject_entity_name='applicationid',type_cd='APPLICATION_DECISION',subject_src_id='11004',status_cd=r.profile(uid,'uw_status',c,UW),decision_status_cd=r.profile(uid,'decision',c,UW),decision_phase_cd=r.profile(uid,'phase',c,UW),subject_type_cd='APPLICATION');u['status_d']=u['created_d']+r.integer(uid,'status offset',0,12*3600000+1);dd=u['status_d']+r.integer(uid,'decision offset',0,12*3600000+1);u['decision_status_d']=pa.array(dd.astype('datetime64[ms]'),mask=u['decision_status_cd']==None,type=TYPES['t']);u['modified_d']=np.maximum(u['modified_d'],np.where(u['decision_status_cd']==None,u['status_d'],dd));u['expiration_d']=u['created_d']+30*MS_DAY;result[UW]=table(UW,u)
        ri,ro=self.children[RUN].expand(uo);rid=self.ids(RUN,ro);run=base(RUN,rid,c,r,u['created_d'][ri]);run.update(uw_process_id=strings(uid[ri]),decision_phase_cd=r.profile(rid,'phase',c,RUN),decision_status_cd=r.profile(rid,'decision',c,RUN));result[RUN]=table(RUN,run)
        fi,fo=self.children[RUNREF].expand(ro);ids=self.ids(RUNREF,fo);d=base(RUNREF,ids,c,r,run['created_d'][fi]);s=c['distributions']['uw_ext_entity'];vals=list(s['values']);weights=list(s['weights']);allps=self.j+(existing.ps_count if existing else 0)
        if not allps:weights=[w if v!='prescreenentry' else 0 for v,w in zip(vals,weights)];require(sum(weights)>0 or not len(ids),'No eligible external entity')
        entities=r.choice(ids,'run entity',vals,weights) if len(ids) else np.array([],object);refs=np.asarray(pc.binary_join_element_wise(pa.scalar('synthetic:'),uuids(ids,r,'run external'),pa.scalar('')).to_pylist(),object);src=np.full(len(ids),'SYNTHETIC_EXTERNAL',object)
        am=entities=='applicantid';appix=ix[ri[fi[am]]];second=joint[appix]&(r.u(ids[am],'run applicant choice')<.5);appord=ao[appix].copy();appord[second]=self.n+self.joint_rank(ao[appix[second]]);refs[am]=strings(self.ids(APPLICANT,appord));src[am]='11004';pm=entities=='prescreenentry'
        if pm.any():
            pick=r.integer(ids[pm],'run prescreen choice',0,allps);old=pick<(existing.ps_count if existing else 0);pids=np.empty(len(pick),np.int64)
            if old.any():pids[old]=existing.prescreens(pick[old])
            pids[~old]=self.ids(PS,pick[~old]-(existing.ps_count if existing else 0));refs[pm]=strings(pids);src[pm]='11000'
        d.update(uw_process_run_id=strings(rid[fi]),ext_ref_id=refs,ext_src_id=src,ext_ref_entity_name=entities);result[RUNREF]=table(RUNREF,d)
        for ch,col,par,sel,val in FKS:
            if (ch==PSREF and col=='ext_ref_id') or (ch==RUNREF and val=='prescreenentry'):continue
            a=result[ch] if sel is None else result[ch].filter(pc.equal(result[ch][sel],val));require(all_pc(pc.is_in(a[col],value_set=result[par]['id'])),'Batch FK '+ch+'.'+col)
        return result

def csv_table(a):
    for i,f in enumerate(a.schema):
        if pa.types.is_timestamp(f.type):a=a.set_column(i,f.name,pc.binary_join_element_wise(pc.strftime(a.column(i),format='%Y-%m-%d %H:%M:%S'),pa.scalar('000'),pa.scalar('')))
    return a
def read_batches(path,t,fmt,cdc=False,batch_size=20000):
    expected=schema(t,cdc)
    if fmt=='parquet':
        pf=pq.ParquetFile(path);require(pf.schema_arrow.remove_metadata().equals(expected),'Parquet physical schema '+t)
        for b in pf.iter_batches(batch_size=batch_size):yield pa.Table.from_batches([b],schema=expected)
    else:
        readtypes=pa.schema([pa.field(f.name,pa.timestamp('ns') if pa.types.is_timestamp(f.type) else f.type) for f in expected]);reader=pacsv.open_csv(path,read_options=pacsv.ReadOptions(block_size=8*1024**2,use_threads=False),convert_options=pacsv.ConvertOptions(column_types=readtypes,null_values=[''],strings_can_be_null=True,quoted_strings_can_be_null=True));require(reader.schema.names==expected.names,'CSV columns '+t)
        for b in reader:
            a=pa.Table.from_batches([b]).cast(expected,safe=True)
            for offset in range(0,len(a),batch_size):yield a.slice(offset,batch_size)
def sha_file(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(8*1024**2),b''):h.update(b)
    return h.hexdigest()
def sidecar(t,cdc=False):
    sql={x.split(':')[0]:SQLT[x.split(':')[1]] for x in CONTRACT[t].split()}
    return {'schema_version':2,'table':t,'kind':'cdc' if cdc else 'snapshot','csv_null_token':'','note':'CSV is untyped; apply this physical contract. Compatibility aliases are separate SQL views, not physical columns. CDC is not a snapshot.','unqualified_decimal_assumption':{'precision':38,'scale':2},'fields':[{'name':f.name,'sql_type':sql.get(f.name,'string'),'arrow_type':str(f.type),'nullable':f.nullable} for f in schema(t,cdc)],'operations':['INSERT','UPDATE','DELETE'] if cdc else None}

# N1 RESOLUTION: schema-qualified views backed by existing files, zero row duplication.
def compatibility_sql(root,fmt):
    root=Path(root).resolve();parts=['-- DuckDB SQL. Physical CSV/Parquet files are unchanged.','-- Public detail view has the additional source_code alias; use _physical_lc20_as for the exact Table_info projection.','BEGIN TRANSACTION;']
    for s in sorted({t.split('.')[0] for t in ORDER}):parts+=['CREATE SCHEMA IF NOT EXISTS '+q('_physical_'+s)+';','CREATE SCHEMA IF NOT EXISTS '+q(s)+';']
    for t in ORDER:
        path=root/relpath(t,fmt);columns=[x.split(':') for x in CONTRACT[t].split()];select=','.join('CAST('+q(n)+' AS '+DBT[k]+') AS '+q(n) for n,k in columns)
        if fmt=='csv':
            defs='{'+','.join(lit(n)+':'+lit(DBT[k]) for n,k in columns)+'}'
            source='read_csv('+lit(path)+", header=true, auto_detect=false, columns="+defs+", nullstr='', timestampformat='%Y-%m-%d %H:%M:%S.%f')"
        else:source='read_parquet('+lit(path)+')'
        parts.append('CREATE OR REPLACE VIEW '+fq(physical(t))+' AS SELECT '+select+' FROM '+source+';')
        suffix=', source_cd AS source_code' if t==DETAIL else ''
        parts.append('CREATE OR REPLACE VIEW '+fq(t)+' AS SELECT *'+suffix+' FROM '+fq(physical(t))+';')
    for alias,target in TABLE_ALIASES.items():parts.append('CREATE OR REPLACE VIEW '+fq(alias)+' AS SELECT * FROM '+fq(target)+';')
    parts.append('COMMIT;');return '\n'.join(parts)+'\n'

def install_compatibility(c):
    import duckdb
    root=Path(c['local_dir']).resolve();mp=root/'_manifest.json'
    if mp.exists():
        m=json.loads(mp.read_text());require(not (m.get('mode')=='incremental' and m.get('incremental_output')=='delta'),'CDC cannot be installed as a relational snapshot')
    for t in ORDER:require((root/relpath(t,c['format'])).exists(),'Missing snapshot file '+t)
    sql=compatibility_sql(root,c['format']);sqlpath=root/'_compatibility.duckdb.sql';sqlpath.write_text(sql);dbpath=root/'_compatibility.duckdb';db=duckdb.connect(str(dbpath))
    try:
        db.execute('SET memory_limit='+lit(c['memory_limit']));db.execute('SET threads='+str(c['threads']));db.execute(sql)
        for t in ORDER:
            actual=db.execute('SELECT * FROM '+fq(physical(t))+' LIMIT 0').fetch_arrow_table().schema
            require(actual.remove_metadata().equals(schema(t)),'Compatibility physical schema '+t)
        for alias,target in TABLE_ALIASES.items():
            require(db.execute('SELECT * FROM '+fq(alias)+' LIMIT 0').fetch_arrow_table().schema.equals(schema(target)),'Unresolved alias '+alias)
        cols=[r[0] for r in db.execute('DESCRIBE '+fq(DETAIL)).fetchall()];require(cols==schema(DETAIL).names+['source_code'],'Column alias projection')
        require(db.execute('SELECT source_code FROM '+fq(DETAIL)+' LIMIT 0').description[0][0]=='source_code','Unresolved source_code')
    finally:db.close()
    meta={'available':True,'engine':'DuckDB','sql_path':str(sqlpath),'database_path':str(dbpath),'table_aliases':TABLE_ALIASES,'column_aliases':COLUMN_ALIASES,'exact_physical_view_schemas':['_physical_'+s for s in sorted({t.split('.')[0] for t in ORDER})],'validated_identifiers':True,'physical_files_unchanged':True,'note':'Open _compatibility.duckdb to use requirement names. Views contain local absolute paths; after moving/downloading files run --install-compatibility --format <format> --local-dir <directory>. No row copies; detail SQL view adds source_code while physical source_cd remains unchanged.'}
    (root/'_compatibility.json').write_text(json.dumps(meta,indent=2));(root/'_compatibility.README.txt').write_text(meta['note']+'\nThis SQL is DuckDB-specific, not a universal migration for other databases.\nCDC deltas must be applied to a snapshot before installing compatibility views.\n')
    return meta

class Outputs:
    def __init__(self,c,cdc=False):
        self.c=c;self.cdc=cdc;self.root=Path(c['local_dir']);self.writers={};self.counts={t:0 for t in ORDER};self.profiles={};self.nulls={};self.closed=False;self.root.mkdir(parents=True,exist_ok=True)
        for t in ORDER:
            path=self.root/relpath(t,c['format']);path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_suffix(path.suffix+'.partial')
            self.writers[t]=pacsv.CSVWriter(str(temp),csv_table(pa.Table.from_batches([],schema=schema(t,cdc))).schema,write_options=pacsv.WriteOptions(include_header=True,batch_size=8192)) if c['format']=='csv' else pq.ParquetWriter(temp,schema(t,cdc),compression='snappy',version='2.6')
    def write(self,t,a):
        validate_table(a,t,self.cdc,self.c if not self.cdc else None);self.counts[t]+=len(a)
        for col,p in PROFILE.get(t,[]):
            d=self.profiles.setdefault((t,col,p),{})
            for item in pc.value_counts(a[col]).to_pylist():d[item['values']]=d.get(item['values'],0)+item['counts']
        for col in schema(t).names:
            if col in NULLABLE:self.nulls[(t,col)]=self.nulls.get((t,col),0)+a[col].null_count
        if self.c['format']=='parquet':self.writers[t].write_table(a,row_group_size=min(self.c['max_batch_rows'],max(1,len(a))))
        else:self.writers[t].write_table(csv_table(a))
    def close(self):
        if not self.closed:
            for w in self.writers.values():w.close()
            self.closed=True
    def finish(self,targets,extra=None):
        self.close();checks=[];frequencies=[]
        for t in ORDER:
            require(self.counts[t]==targets[t],'Global row target '+t);path=self.root/relpath(t,self.c['format']);temp=path.with_suffix(path.suffix+'.partial');count=0
            for a in read_batches(temp,t,self.c['format'],self.cdc,self.c['max_batch_rows']):validate_table(a,t,self.cdc,self.c if not self.cdc else None);count+=len(a)
            require(count==targets[t],'Read-back rows '+t);checks.append({'table':t,'rows':count,'passed':True,'sha256':sha_file(temp)})
        for (t,col,p),counts in self.profiles.items():
            n=self.counts[t]
            for value,prob in zip(self.c['distributions'][p]['values'],self.c['distributions'][p]['weights']):
                observed=counts.get(value,0)/n if n else 0;tol=max(.08,4*math.sqrt(prob*(1-prob)/n)+1/n) if n else 1;passed=abs(observed-prob)<=tol
                if self.c['mode']=='full' and n:require(passed,'Distribution drift '+col)
                frequencies.append({'table':t,'column':col,'value':value,'expected':prob,'observed':observed,'passed':passed})
        for t in ORDER:
            path=self.root/relpath(t,self.c['format']);os.replace(path.with_suffix(path.suffix+'.partial'),path);path.with_name(t.split('.')[1]+'.schema.json').write_text(json.dumps(sidecar(t,self.cdc),indent=2))
        # Remove a prior manifest before installing newly generated snapshot views.
        mp=self.root/'_manifest.json'
        if mp.exists():mp.unlink()
        if not self.cdc:compat=install_compatibility(self.c)
        else:
            compat={'available':False,'reason':'CDC is not a relational snapshot. Apply it to a snapshot, then install compatibility views.'}
            for name in ['_compatibility.duckdb.sql','_compatibility.duckdb','_compatibility.json','_compatibility.README.txt']:(self.root/name).unlink(missing_ok=True)
        manifest={'schema_version':2,'compatibility_version':1,'mode':self.c['mode'],'incremental_output':self.c['incremental_output'],'configuration':self.c,'assumptions':ASSUMPTIONS,'tables':{t:{'rows':self.counts[t],'columns':len(schema(t,self.cdc)),'local_path':str(self.root/relpath(t,self.c['format']))} for t in ORDER},'serialized_schema_checks':checks,'distribution_checks':frequencies,'null_fractions':{t:{col:(self.nulls.get((t,col),0)/self.counts[t] if self.counts[t] else None) for col in schema(t).names if col in NULLABLE} for t in ORDER},'validation':{'constraints_passed':True,'modeled_foreign_keys_passed':True,'row_counts_passed':True,'streamed_readback_passed':True},'naming_compatibility':compat,'locations':[]}
        if extra:manifest.update(extra)
        if not self.c['no_upload']:
            import boto3
            from boto3.s3.transfer import TransferConfig
            client=boto3.client('s3');transfer=TransferConfig(multipart_threshold=64*1024**2,multipart_chunksize=64*1024**2,max_concurrency=min(4,self.c['threads']),use_threads=True)
            for t in ORDER:
                path=self.root/relpath(t,self.c['format']);key=self.c['prefix']+relpath(t,self.c['format']).as_posix();client.upload_file(str(path),self.c['bucket'],key,Config=transfer);body=client.get_object(Bucket=self.c['bucket'],Key=key)['Body'];h=hashlib.sha256()
                try:
                    for block in iter(lambda:body.read(8*1024**2),b''):h.update(block)
                finally:body.close()
                require(h.hexdigest()==next(x['sha256'] for x in checks if x['table']==t),'S3 read-back '+t);sc=path.with_name(t.split('.')[1]+'.schema.json');client.upload_file(str(sc),self.c['bucket'],self.c['prefix']+sc.relative_to(self.root).as_posix(),Config=transfer);manifest['locations'].append('s3://'+self.c['bucket']+'/'+key)
            if compat['available']:
                for name in ['_compatibility.duckdb.sql','_compatibility.json','_compatibility.README.txt']:client.upload_file(str(self.root/name),self.c['bucket'],self.c['prefix']+name,Config=transfer)
        mp.write_text(json.dumps(manifest,indent=2,default=str))
        if not self.c['no_upload']:client.upload_file(str(mp),self.c['bucket'],self.c['prefix']+'_manifest.json',Config=transfer)
        return manifest

def generate_full(c,n=None,starts=None,existing=None,sink=None):
    n=int(round(c['applications']*c['scale_factor'])) if n is None else n;p=Plan(c,n,starts);own=sink is None;out=Outputs(c) if own else sink
    try:
        for start in range(0,n,p.batch):
            a=p.batch_data(start,min(n,start+p.batch),existing)
            for t in ORDER:out.write(t,a[t])
        if own:return out.finish(p.total,{'generation_batch_size':p.batch,'validation_method':'Ordinal-range PK/FK construction, batch FK checks, streamed value/schema read-back, and compatibility identifier binding.'})
        return p.total
    finally:
        if own:out.close()

def validate_relations(db,prefix):
    names={t:q(dbname(prefix,t)) for t in ORDER}
    def check(sql,msg):require(db.execute(sql).fetchone()[0]==0,msg)
    for t in ORDER:check(f'SELECT count(*)-count(DISTINCT id) FROM {names[t]}','Global PK '+t)
    for ch,col,par,sel,val in FKS:
        cond='TRUE' if sel is None else 'c.'+q(sel)+'='+lit(val);check(f'SELECT count(*) FROM {names[ch]} c WHERE {cond} AND NOT EXISTS(SELECT 1 FROM {names[par]} p WHERE p.id=c.{q(col)})','Global FK '+ch+'.'+col)
    apps=names[APP];ap=names[APPLICANT];oref=names[ORIGINREF];psref=names[PSREF]
    check(f"SELECT count(*) FROM {apps} a LEFT JOIN (SELECT application_id,count(*) n,sum(primary_applicant_flg) pri FROM {ap} GROUP BY 1) p ON a.id=p.application_id WHERE coalesce(p.n,0)<>CASE WHEN a.application_type_cd LIKE 'JOINT_%' THEN 2 ELSE 1 END OR coalesce(p.pri,0)<>1",'Applicant coverage')
    for t,(fk,par) in GROUPS.items():check(f'SELECT count(*) FROM {names[par]} p WHERE NOT EXISTS(SELECT 1 FROM {names[t]} c WHERE c.{q(fk)}=p.id)','Required child '+t)
    for t,(types,_,_,_) in ACCOUNTS.items():
        vals=','.join(lit(DISTRIBUTIONS['application_type']['values'][k]) for k in types);check(f'SELECT count(*) FROM {apps} a LEFT JOIN (SELECT application_id,count(*) n FROM {names[t]} GROUP BY 1) x ON a.id=x.application_id WHERE coalesce(x.n,0)<>CASE WHEN a.application_type_cd IN ({vals}) THEN 1 ELSE 0 END','Account eligibility')
        check(f"SELECT count(*) FROM {names[t]} x JOIN {apps} a ON x.application_id=a.id WHERE x.is_joint_flg<>CASE WHEN a.application_type_cd LIKE 'JOINT_%' THEN 1 ELSE 0 END",'Joint flag')
    check(f"SELECT count(*) FROM {apps} a WHERE (SELECT count(*) FROM {oref} r WHERE r.ext_entity_name='applicationid' AND r.ext_ref_id=a.id)<>1",'Application origination')
    check(f"SELECT count(*) FROM {names[ORIGIN]} o WHERE (SELECT count(*) FROM {oref} r WHERE r.ext_entity_name='applicationid' AND r.origination_id=o.id)<>1",'Origination coverage')
    check(f"SELECT count(*) FROM {ap} a WHERE (SELECT count(*) FROM {oref} r WHERE r.ext_entity_name='applicantid' AND r.ext_ref_id=a.id)<>1",'Applicant reference')
    check(f"SELECT count(*) FROM {oref} r JOIN {ap} a ON a.id=r.ext_ref_id JOIN {oref} ar ON ar.origination_id=r.origination_id AND ar.ext_entity_name='applicationid' WHERE r.ext_entity_name='applicantid' AND a.application_id<>ar.ext_ref_id",'Applicant origination consistency')
    check(f"SELECT count(*) FROM {apps} a JOIN {oref} ar ON ar.ext_ref_id=a.id AND ar.ext_entity_name='applicationid' LEFT JOIN (SELECT origination_id,count(*) n FROM {oref} WHERE ext_entity_name='prescreenentry' GROUP BY 1) p ON p.origination_id=ar.origination_id WHERE coalesce(p.n,0)<>CASE WHEN a.application_type_cd LIKE 'JOINT_%' THEN 1 ELSE 0 END",'Joint prescreen coverage')
    check(f"SELECT count(*) FROM {names[PS]} p WHERE (SELECT count(*) FROM {oref} r WHERE r.ext_entity_name='prescreenentry' AND r.ext_ref_id=p.id)<>1 OR (SELECT count(*) FROM {psref} r WHERE r.ps_prescreen_entry_id=p.id)<>1",'Prescreen coverage')
    check(f"SELECT count(*) FROM {oref} pr JOIN {oref} ar ON ar.origination_id=pr.origination_id AND ar.ext_entity_name='applicationid' JOIN {apps} j ON j.id=ar.ext_ref_id JOIN {psref} sr ON sr.ps_prescreen_entry_id=pr.ext_ref_id JOIN {apps} p ON p.id=sr.ext_ref_id WHERE pr.ext_entity_name='prescreenentry' AND p.application_type_cd IS DISTINCT FROM substr(j.application_type_cd,7)",'Same-product primary')
    for t,col in [(ORIGIN,'origination_guid'),(EMAIL,'email_address')]:check(f'SELECT count(*)-count(DISTINCT {q(col)}) FROM {names[t]}','Global unique '+col)

# Independent N1 tests run the conflicted requirement identifiers literally.
def validate_literal_names(db,expected):
    for alias,target in TABLE_ALIASES.items():
        require(db.execute('SELECT count(*) FROM '+fq(alias)).fetchone()[0]==expected[target],'Literal alias '+alias)
        for left,right in [(alias,target),(target,alias)]:
            require(db.execute('SELECT count(*) FROM ((SELECT * FROM '+fq(left)+') EXCEPT ALL (SELECT * FROM '+fq(right)+'))').fetchone()[0]==0,'Alias values differ '+alias)
    require(db.execute('SELECT count(*) FROM lc20_as.as_applicant_detail WHERE source_code IS DISTINCT FROM source_cd OR source_code IS DISTINCT FROM \'APPLICANT\'').fetchone()[0]==0,'Literal source_code')
    mapping="""SELECT j.id AS joint_id,p.id AS primary_id,j.application_type_cd joint_type,p.application_type_cd primary_type
        FROM lc20_as.as_application j
        JOIN lc20_das.das_origination_ext_ref doer1 ON doer1.ext_ref_id=j.id AND doer1.ext_src_id='11004' AND doer1.ext_entity_name='applicationid'
        JOIN lc20_das.das_origination_ext_ref doer2 ON doer2.origination_id=doer1.origination_id AND doer2.ext_src_id='11000' AND doer2.ext_entity_name='prescreenentry'
        JOIN lc20_as.ps_prescreen_ext_ref sr ON sr.ps_prescreen_entry_id=doer2.ext_ref_id AND sr.ext_entity_name='applicationid' AND sr.ext_src_id='11004' AND sr.ext_ref_direction_cd='SOURCE'
        JOIN lc20_as.as_application p ON p.id=sr.ext_ref_id
        WHERE j.application_type_cd LIKE 'JOINT_%'"""
    n=db.execute('SELECT count(*) FROM ('+mapping+')').fetchone()[0];require(n==expected[PS],'Literal prescreen join coverage')
    require(db.execute('SELECT count(*) FROM ('+mapping+") WHERE primary_type IS DISTINCT FROM substr(joint_type,7)").fetchone()[0]==0,'Literal same-product primary')
    actors="""SELECT a.id,m.member_ref_id FROM lc20_as.as_applicant a
        JOIN lc20_as.das_origination_ext_ref r ON r.ext_ref_id=a.id AND r.ext_entity_name='applicantid'
        JOIN lc20_as.das_member_origination m ON m.origination_id=r.origination_id"""
    require(db.execute('SELECT count(DISTINCT id) FROM ('+actors+') WHERE member_ref_id IS NOT NULL').fetchone()[0]==expected[APPLICANT],'Literal actor lookup')
    n=db.execute("SELECT count(*) FROM lc20_as.as_application a JOIN lc20_as.as_application_savings_account s ON s.application_id=a.id WHERE a.application_type_cd IN ('SAVINGS_ACCOUNT','JOINT_SAVINGS_ACCOUNT')").fetchone()[0];require(n==expected[SAV],'Literal savings join')
    return {'passed':True,'table_aliases_checked':len(TABLE_ALIASES),'alias_values_identical':True,'source_code_alias_checked':True,'joint_primary_mappings':expected[PS],'applicants_with_actor_ids':expected[APPLICANT]}

class DatabaseSink:
    def __init__(self,db,prefix):self.db=db;self.prefix=prefix
    def write(self,t,a):
        self.db.register('_incoming',a)
        try:self.db.execute(f'INSERT INTO {q(dbname(self.prefix,t))} SELECT * FROM _incoming')
        finally:self.db.unregister('_incoming')
class ExistingIndex:
    def __init__(self,db,root,batch_size):
        self.primary_counts=[];self.catalogs=[];self.maps=[];root=Path(root);root.mkdir(parents=True,exist_ok=True)
        def build(path,sql,n,width):
            if not n:return np.empty((0,width),np.int64)
            m=np.memmap(path,dtype=np.int64,mode='w+',shape=(n,width));offset=0
            for batch in db.execute(sql).fetch_record_batch(batch_size):
                a=pa.Table.from_batches([batch]);length=len(a)
                for j,col in enumerate(a.column_names):m[offset:offset+length,j]=numeric(a,col)
                offset+=length
            require(offset==n,'Index rows');m.flush();m._mmap.close();m=np.memmap(path,dtype=np.int64,mode='r',shape=(n,width));self.maps.append(m);return m
        for k in range(3):
            cond='application_type_cd='+lit(DISTRIBUTIONS['application_type']['values'][k]);name=q(dbname('w',APP));n=db.execute(f'SELECT count(*) FROM {name} WHERE {cond}').fetchone()[0];self.primary_counts.append(n);self.catalogs.append(build(root/('primary'+str(k)+'.bin'),f'SELECT CAST(id AS BIGINT) id,created_d FROM {name} WHERE {cond} ORDER BY CAST(id AS BIGINT)',n,2))
        name=q(dbname('w',PS));self.ps_count=db.execute(f'SELECT count(*) FROM {name}').fetchone()[0];self.ps=build(root/'prescreens.bin',f'SELECT CAST(id AS BIGINT) id FROM {name} ORDER BY CAST(id AS BIGINT)',self.ps_count,1)
    def primary(self,k,rank):a=self.catalogs[k][rank];return a[:,0],a[:,1]
    def prescreens(self,rank):return self.ps[rank,0]
    def close(self):
        for m in self.maps:m._mmap.close()

def generate_incremental(c):
    import duckdb, shutil
    root=Path(c['local_dir']);root.mkdir(parents=True,exist_ok=True);require(not c['existing_local_dir'] or Path(c['existing_local_dir']).resolve()!=root.resolve(),'Separate incremental output directory required');work=root/'_work';work.mkdir(exist_ok=False);index=None;db=duckdb.connect(str(work/'state.duckdb'));db.execute('SET memory_limit='+lit(c['memory_limit']));db.execute('SET threads='+str(c['threads']));db.execute('SET temp_directory='+lit(work/'spill'));db.execute('SET preserve_insertion_order=false')
    try:
        source=Path(c['existing_local_dir']) if c['existing_local_dir'] else work/'download';source.mkdir(parents=True,exist_ok=True)
        if not c['existing_local_dir']:
            import boto3
            client=boto3.client('s3')
            for t in ORDER:
                path=source/relpath(t,c['input_format']);path.parent.mkdir(parents=True,exist_ok=True);client.download_file(c['bucket'],c['existing_prefix']+relpath(t,c['input_format']).as_posix(),str(path))
        counts={};starts={};stats={};load=DatabaseSink(db,'b')
        for t in ORDER:
            db.register('_empty',pa.Table.from_batches([],schema=schema(t)));db.execute(f'CREATE TABLE {q(dbname("b",t))} AS SELECT * FROM _empty');db.unregister('_empty');n=0
            for a in read_batches(source/relpath(t,c['input_format']),t,c['input_format'],batch_size=c['max_batch_rows']):validate_table(a,t);load.write(t,a);n+=len(a)
            counts[t]=n;starts[t]=db.execute(f'SELECT coalesce(max(CAST(id AS BIGINT)),0)+1 FROM {q(dbname("b",t))}').fetchone()[0];db.execute(f'CREATE TABLE {q(dbname("w",t))} AS SELECT * FROM {q(dbname("b",t))}');db.execute(f'CREATE TABLE {q(dbname("i",t))} AS SELECT * FROM {q(dbname("b",t))} LIMIT 0');stats[t]={'requested_delete':int(round(n*c['delete_pct']/100)),'delete':0,'update':0,'insert':0}
        validate_relations(db,'b')
        for t in reversed(ORDER):
            name=q(dbname('w',t));refs=[]
            for ch,col,par,sel,val in FKS:
                if par==t:
                    cond='TRUE' if sel is None else 'r.'+q(sel)+'='+lit(val);refs.append(f'NOT EXISTS(SELECT 1 FROM {q(dbname("w",ch))} r WHERE {cond} AND r.{q(col)}=x.id)')
            cond=' AND '.join(refs) or 'TRUE';wanted=stats[t]['requested_delete'] if t not in PROTECTED else 0;score=f'hash(x.id,{salt(c["seed"],"delete "+t)}::UBIGINT)';selection=f'SELECT x.* FROM {name} x WHERE {cond}'
            if t in GROUPS:
                fk,_=GROUPS[t];selection=f'SELECT x.* FROM {name} x JOIN (SELECT {q(fk)},count(*)-1 capacity FROM {name} GROUP BY 1) g USING ({q(fk)}) WHERE {cond} QUALIFY row_number() OVER (PARTITION BY x.{q(fk)} ORDER BY {score},CAST(x.id AS BIGINT))<=g.capacity'
            selection+=f' ORDER BY {score},CAST(x.id AS BIGINT) LIMIT {wanted}';deleted=q(dbname('d',t));db.execute(f"CREATE TABLE {deleted} AS SELECT *, 'DELETE'::VARCHAR AS _operation FROM ({selection})");stats[t]['delete']=db.execute(f'SELECT count(*) FROM {deleted}').fetchone()[0];db.execute(f'DELETE FROM {name} WHERE id IN(SELECT id FROM {deleted})')
        for t in ORDER:
            name=q(dbname('w',t));n=int(round((counts[t]-stats[t]['delete'])*c['update_pct']/100));stats[t]['update']=n;db.execute(f'CREATE TEMP TABLE chosen AS SELECT id FROM {name} ORDER BY hash(id,{salt(c["seed"],"update "+t)}::UBIGINT),CAST(id AS BIGINT) LIMIT {n}')
            db.execute(f"UPDATE {name} SET version=version+1,modified_d=CAST(epoch_ms(greatest(epoch_ms(modified_d),{c['date_end_ms']})+1+CAST(hash(id,{salt(c['seed'],'update offset '+t)}::UBIGINT)%{30*MS_DAY} AS BIGINT)) AS TIMESTAMP_MS) WHERE id IN(SELECT id FROM chosen)");db.execute(f"CREATE TABLE {q(dbname('u',t))} AS SELECT *, 'UPDATE'::VARCHAR AS _operation FROM {name} WHERE id IN(SELECT id FROM chosen)");db.execute('DROP TABLE chosen')
        index=ExistingIndex(db,work/'indexes',c['max_batch_rows']);targets=generate_full(c,int(round(counts[APP]*c['insert_pct']/100)),starts,index,DatabaseSink(db,'i'))
        for t in ORDER:
            stats[t]['insert']=targets[t];db.execute(f'INSERT INTO {q(dbname("w",t))} SELECT * FROM {q(dbname("i",t))}');db.execute(f"CREATE VIEW {q(dbname('c',t))} AS SELECT * FROM {q(dbname('d',t))} UNION ALL SELECT * FROM {q(dbname('u',t))} UNION ALL SELECT *, 'INSERT'::VARCHAR AS _operation FROM {q(dbname('i',t))}")
        validate_relations(db,'w')
        for t in ORDER:
            b=q(dbname('b',t));w=q(dbname('w',t));d=q(dbname('d',t));u=q(dbname('u',t));i=q(dbname('i',t));cols=','.join(q(f.name) for f in schema(t));delta=q(dbname('c',t));require(db.execute(f'SELECT count(*)-count(DISTINCT id) FROM {delta}').fetchone()[0]==0,'CDC PK');require(db.execute(f'SELECT count(*) FROM {i} JOIN {b} USING(id)').fetchone()[0]==0,'Insert disjointness')
            replay=f'SELECT {cols} FROM {b} WHERE id NOT IN(SELECT id FROM {d} UNION ALL SELECT id FROM {u}) UNION ALL SELECT {cols} FROM {u} UNION ALL SELECT {cols} FROM {i}'
            for left,right in [(replay,f'SELECT * FROM {w}'),(f'SELECT * FROM {w}',replay)]:require(db.execute(f'SELECT count(*) FROM (({left}) EXCEPT ALL ({right}))').fetchone()[0]==0,'CDC replay '+t)
        cdc=c['incremental_output']=='delta';out=Outputs(c,cdc);outtargets={}
        try:
            for t in ORDER:
                for batch in db.execute(f'SELECT * FROM {q(dbname("w",t))}').fetch_record_batch(c['max_batch_rows']):validate_table(pa.Table.from_batches([batch]).cast(schema(t),safe=True),t)
                chosen=q(dbname('c' if cdc else 'w',t));outtargets[t]=db.execute(f'SELECT count(*) FROM {chosen}').fetchone()[0]
                for batch in db.execute(f'SELECT * FROM {chosen} ORDER BY CAST(id AS BIGINT)').fetch_record_batch(c['max_batch_rows']):out.write(t,pa.Table.from_batches([batch]).cast(schema(t,cdc),safe=True))
            return out.finish(outtargets,{'incremental_operations':stats,'merged_row_counts':{t:counts[t]-stats[t]['delete']+targets[t] for t in ORDER},'validation_method':'Disk-backed global relations, value checks, and bidirectional EXCEPT ALL CDC replay.'})
        finally:out.close()
    finally:
        if index is not None:index.close()
        db.close()
        if not c['keep_work']:shutil.rmtree(work)

def self_test(c,manifest):
    import duckdb
    root=Path(c['local_dir']);data={};db=duckdb.connect();require(len(ORDER)==15 and sum(len(s) for s in SCHEMAS.values())==171,'Independent contract size')
    for t in ORDER:
        parts=list(read_batches(root/relpath(t,c['format']),t,c['format']));a=pa.concat_tables(parts) if parts else pa.Table.from_batches([],schema=schema(t));data[t]=a;db.register(dbname('test',t),a);path=root/'_roundtrip'
        pq.write_table(a,path,compression='snappy');require(pq.read_schema(path).equals(schema(t)) and pq.read_table(path).equals(a),'Exact Parquet');path.unlink()
        with pacsv.CSVWriter(str(path),csv_table(a).schema) as w:w.write_table(csv_table(a))
        parts=list(read_batches(path,t,'csv'));b=pa.concat_tables(parts) if parts else pa.Table.from_batches([],schema=schema(t));require(b.equals(a),'Exact typed CSV');path.unlink()
    validate_relations(db,'test');db.close()
    for t,col,bad_type in [(APPLICANT,'primary_applicant_flg',pa.int64()),(APP,'product_type_cd',pa.null()),(CD,'apy',pa.decimal128(3,2))]:
        expected=schema(t);bad=expected.set(expected.get_field_index(col),pa.field(col,bad_type));empty=pa.Table.from_batches([],schema=bad)
        try:validate_table(empty,t)
        except ValueError:pass
        else:raise ValueError('Schema mismatch was not rejected: '+col)
    p=Plan(c,1000);cc=copy.deepcopy(c);cc['batch_size']=137;p2=Plan(cc,1000);pieces={t:[] for t in ORDER}
    for start in range(0,1000,p2.batch):
        a=p2.batch_data(start,min(1000,start+p2.batch))
        for t in ORDER:pieces[t].append(a[t])
    for t in ORDER:
        b=pa.concat_tables(pieces[t]);require(data[t].take(pc.sort_indices(pc.cast(data[t]['id'],pa.int64()))).equals(b.take(pc.sort_indices(pc.cast(b['id'],pa.int64())))),'Batch reproducibility')
        for cdc in [False,True]:
            empty=pa.Table.from_batches([],schema=schema(t,cdc));path=root/'_empty'
            with pacsv.CSVWriter(str(path),csv_table(empty).schema) as w:w.write_table(csv_table(empty))
            require(sum(len(a) for a in read_batches(path,t,'csv',cdc))==0,'Empty CSV');path.unlink();pq.write_table(empty,path);require(pq.read_schema(path).equals(schema(t,cdc)),'Empty Parquet');path.unlink()
    db=duckdb.connect(str(root/'_compatibility.duckdb'),read_only=True)
    try:literal=validate_literal_names(db,p.total)
    finally:db.close()
    # The emitted SQL must independently recreate all views, not merely document names.
    db=duckdb.connect()
    try:db.execute((root/'_compatibility.duckdb.sql').read_text());validate_literal_names(db,p.total)
    finally:db.close()
    inc=copy.deepcopy(c);inc.update(mode='incremental',existing_local_dir=c['local_dir'],input_format=c['format'],incremental_output='delta',local_dir=str(root/'_cdc_test'),no_upload=True,keep_work=False,self_test=False);delta=generate_incremental(inc);require(not delta['naming_compatibility']['available'],'CDC must not masquerade as snapshot')
    manifest['self_tests']={'applications':1000,'physical_tables':15,'physical_columns':171,'modeled_fk_checks':len(FKS),'global_relationships':True,'exact_csv_parquet_roundtrips':True,'batch_independent_reproducibility':True,'empty_outputs':True,'negative_schema_tests':True,'literal_naming_compatibility':literal,'emitted_sql_executed':True,'incremental_fk_safety_and_cdc_replay':True,'cdc_rows':sum(v['rows'] for v in delta['tables'].values())};(root/'_manifest.json').write_text(json.dumps(manifest,indent=2,default=str));print('SELF_TESTS_PASSED='+json.dumps(manifest['self_tests']))

def main(argv=None):
    c=parse_config(argv)
    if c['install_compatibility']:
        meta=install_compatibility(c);mp=Path(c['local_dir'])/'_manifest.json'
        if mp.exists():m=json.loads(mp.read_text());m['naming_compatibility']=meta;mp.write_text(json.dumps(m,indent=2))
        print('COMPATIBILITY_INSTALLED='+json.dumps(meta));return meta
    m=generate_full(c) if c['mode']=='full' else generate_incremental(c)
    if c['self_test']:self_test(c,m)
    print('SUMMARY_JSON='+json.dumps({'applications':m['tables'][APP]['rows'],'total_rows':sum(v['rows'] for v in m['tables'].values()),'tables':15,'format':c['format'],'mode':c['mode'],'local_dir':c['local_dir'],'naming_compatibility':m['naming_compatibility']['available'],'validation_passed':True,'uploaded':not c['no_upload']}));return m

if __name__=='__main__':
    notebook=Path(sys.argv[0]).name in {'ipykernel_launcher.py','ipykernel'}
    main(['--applications','1000','--batch-size','250','--no-upload','--local-dir','/tmp/lc_synthetic_revised','--self-test'] if notebook else None)
