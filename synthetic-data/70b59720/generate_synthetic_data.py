# Synthetic banking database generator.
# NAMING RESOLUTION: catalog names win. as_application_savings_account aliases
# catalog lc20_as.as_application_saving_account. Misqualified lc20_as aliases for
# ps_prescreen_ext_ref and DAS tables resolve to lc20_ps and lc20_das. Enum
# source_code resolves to catalog source_cd. Whitespace is stripped and NULL/null
# enum tokens are real nulls. "No Values" supplies no closed domain.
# POLYMORPHIC EXCLUSIONS: DAS external-only labels bpsrequestid, borroweractorid,
# loanapp, coapplicantid, address, coborroweractorid, actor; prescreen external-only
# labels actor, loan, referrer, product, bpsrequestid, loanapp, loginactorid,
# bankaccount, productcode, accounttype, cif, program; UW subject address/actor;
# UW-run external-only narmiuuid, journeyapplicationtoken, cif, loginactorid,
# entitytoken, offerid. DAS member_ref_id is an external actor identifier.
# CDC: _operation is a delta-only extension, stripped when reloaded.
import io,os,random,tempfile
from decimal import Decimal,ROUND_HALF_UP
import boto3,numpy as np,pandas as pd
from faker import Faker
SEED=42; SCALE_FACTOR=.20; S3_BUCKET="mm-fsi-fix"; S3_PREFIX="lc/synthetic_data_1/"; OUTPUT_FORMAT="csv"; UPLOAD_ENABLED=True; MODE="full"; INCREMENTAL_OUTPUT="merged"; INSERT_PCT=10.; UPDATE_PCT=5.; DELETE_PCT=2.; ROOT_ROW_COUNTS={"lc20_as.as_application":1000}; SELF_TEST_APPLICATION_ROWS=140
O=["lc20_as.as_application","lc20_as.as_applicant","lc20_as.as_application_saving_account","lc20_as.as_application_checking_account","lc20_as.as_application_certificate_of_deposit_account","lc20_as.as_applicant_detail","lc20_as.as_email_address","lc20_uw_processing.uw_process","lc20_uw_processing.uw_process_run","lc20_uw_processing.uw_process_run_ext_ref","lc20_das.das_origination","lc20_das.das_member_origination","lc20_das.das_origination_ext_ref","lc20_ps.ps_prescreen_entry","lc20_ps.ps_prescreen_ext_ref"]
CS=["id created_d modified_d version application_d product_type_cd application_type_cd","id created_d modified_d version application_id primary_applicant_flg sms_marketing_opt_in_flg marketing_attribution_cd","id created_d modified_d version source_cd application_id initial_deposit_amount product_code is_joint_flg debit_card_requested_flg account_type","id created_d modified_d version application_id source_cd debit_card_requested_flg min_initial_deposit_amount initial_deposit_amount product_code is_joint_flg account_type","id created_d modified_d version source_cd initial_deposit_amount product_code application_id apy term_in_months is_joint_flg account_type","id created_d modified_d version applicant_id source_cd first_name first_name_keyn first_name_keyv last_name last_name_keyn last_name_keyv honorific_cd date_of_birth date_of_birth_keyn date_of_birth_keyv ssn ssn_keyn ssn_keyv marital_status_cd educational_qualification_cd employment_status_cd cumulative_job_tenure_months annual_income annual_additional_income annual_income_taxable annual_income_non_taxable housing_status_cd annual_housing_expense annual_additional_debt fico_range_cd fico_min fico_max citizenship","id created_d modified_d version applicant_id source_cd email_address email_address_keyn email_address_keyv","id created_d modified_d version subject_ref_id subject_entity_name type_cd subject_src_id status_cd product_type_cd status_d decision_status_cd decision_status_d decision_phase_cd subject_type_cd expiration_d","id created_d modified_d version uw_process_id decision_phase_cd decision_status_cd","id created_d modified_d version uw_process_run_id ext_ref_id ext_src_id ext_ref_entity_name","id created_d modified_d version origination_guid","id created_d modified_d version origination_id member_ref_id member_src_id member_entity_name","id created_d modified_d version origination_id ext_ref_id ext_src_id ext_entity_name flow_id flow_version","id created_d modified_d version source_cd credit_policy_name credit_policy_version effective_start_d effective_end_d status_cd first_look_up last_look_up used_d offer_amt offer_purpose_cd","id created_d modified_d version ps_prescreen_entry_id ext_src_id ext_ref_id ext_entity_name ext_ref_direction_cd"]
TS=["s t t i t s s","s t t i s i i s","s t t i s s s s i i s","s t t i s s i s s s i s","s t t i s s s s d i i s","s t t i s s s s i s s i s s s i s s i s s s s s s s s s s s s s s i","s t t i s s s s i","s t t i s s s s s s t s t s s t","s t t i s s s","s t t i s s s s","s t t i s","s t t i s s s s","s t t i s s s s s i","s t t i s s i t t s t t t d s","s t t i s s s s s"]
C={t:s.split() for t,s in zip(O,CS)}; T={t:s.split() for t,s in zip(O,TS)}; P=dict(zip(O,"APP APL SAV CHK CDA APD EML UWP UWR URE ORG DMO DOE PSE PER".split()))
AT="SAVINGS_ACCOUNT CHECKING_ACCOUNT CERTIFICATE_DEPOSIT_ACCOUNT JOINT_SAVINGS_ACCOUNT JOINT_CERTIFICATE_DEPOSIT_ACCOUNT JOINT_CHECKING_ACCOUNT".split()+[None]; EMP="UNEMPLOYED EMPLOYED RETIRED SELFEMPLOYED DISABLED OTHER".split()+[None]; HOUSE="OWN MORTGAGE RENT NONE ANY OTHER".split()+[None]; UWT=["APPLICATION_DECISION","ADDRESS_VALIDATION",None]; UWS="FAILURE CANCELED SUCCESS EXPIRED WITHDRAWN ACTIVE NEW".split(); DS=[None]+"FAILURE ERROR IN_PROGRESS SUCCESS MORE_INFO PENDING".split(); PH="FULL_DECISION ADDRESS_VALIDATION BANK_ACCOUNT FULFILLMENT".split()+[None]
ENUM={(O[5],"source_cd"):["APPLICANT"],(O[5],"employment_status_cd"):EMP,(O[5],"housing_status_cd"):HOUSE,(O[0],"application_type_cd"):AT,(O[4],"source_cd"):["APPLICANT"],(O[4],"product_code"):"14 26 25 22 06 12 21 23 28".split(),(O[4],"account_type"):["CD"],(O[3],"source_cd"):["APPLICANT"],(O[3],"product_code"):"53 01".split(),(O[3],"account_type"):["ND"],(O[2],"source_cd"):["APPLICANT"],(O[2],"product_code"):"09 10 25".split(),(O[2],"account_type"):["SV"],(O[6],"source_cd"):["APPLICANT"],(O[11],"member_src_id"):"1000 1200".split(),(O[11],"member_entity_name"):"loginactorid pfloginuserid".split(),(O[12],"ext_entity_name"):"prescreenentry bpsrequestid borroweractorid loanapp applicantid coapplicantid applicationid address coborroweractorid actor depositaccount".split(),(O[14],"ext_entity_name"):"actor loan referrer product bpsrequestid loanapp loginactorid bankaccount productcode accounttype cif applicationid program".split(),(O[14],"ext_ref_direction_cd"):["TARGET","SOURCE",None],(O[7],"subject_entity_name"):"applicationid address actor".split(),(O[7],"type_cd"):UWT,(O[7],"status_cd"):UWS,(O[7],"decision_status_cd"):DS,(O[8],"decision_phase_cd"):PH,(O[8],"decision_status_cd"):DS,(O[9],"ext_ref_entity_name"):"narmiuuid bankaccount prescreenentry journeyapplicationtoken applicantid cif loginactorid entitytoken offerid".split()}
EX={(O[12],"ext_entity_name"):set("bpsrequestid borroweractorid loanapp coapplicantid address coborroweractorid actor".split()),(O[14],"ext_entity_name"):set("actor loan referrer product bpsrequestid loanapp loginactorid bankaccount productcode accounttype cif program".split()),(O[7],"subject_entity_name"):{"address","actor"},(O[9],"ext_ref_entity_name"):set("narmiuuid journeyapplicationtoken cif loginactorid entitytoken offerid".split())}
PIN={(O[0],"product_type_cd"),(O[5],"source_cd"),(O[6],"source_cd"),(O[2],"source_cd"),(O[2],"account_type"),(O[3],"source_cd"),(O[3],"account_type"),(O[4],"source_cd"),(O[4],"account_type"),(O[7],"subject_entity_name"),(O[7],"subject_src_id"),(O[14],"ext_entity_name"),(O[14],"ext_src_id")}
random.seed(SEED); Faker.seed(SEED); fake=Faker(); fake.seed_instance(SEED)
def q(x): return Decimal(str(x)).quantize(Decimal(".01"),rounding=ROUND_HALF_UP)
def tm(x): return pd.Timestamp(x).floor("ms")
def aud(p,r):
 c=tm(p+pd.Timedelta(hours=int(r.integers(0,48)))); return c,tm(c+pd.Timedelta(hours=int(r.integers(1,121))))
def frame(rows,t): return pd.DataFrame(rows,columns=C[t])
def cnt(n,avg,r):
 a=np.ones(n,dtype=int); ix=r.permutation(n)
 for k in range(int(round(n*avg))-n): a[ix[k%n]]+=1
 return a
class IDs:
 def __init__(self,start=None): self.n={t:int((start or {}).get(t,1)) for t in O}
 def get(self,t): n=self.n[t]; self.n[t]+=1; return f"{P[t]}{n:08d}"
def norm(d):
 for t,x in d.items():
  for c,y in zip(C[t],T[t]):
   if y=="t": x[c]=pd.to_datetime(x[c]).dt.floor("ms")
 return d
def generate(n,seed,start=None):
 r=np.random.default_rng(seed); f=Faker(); f.seed_instance(seed); ids=IDs(start); z={t:[] for t in O}; base=pd.Timestamp("2022-01-01")
 for i in range(n):
  c=tm(base+pd.Timedelta(days=int(r.integers(0,730)),hours=int(r.integers(0,24)))); ad=tm(c+pd.Timedelta(hours=int(r.integers(1,73)))); m=tm(ad+pd.Timedelta(hours=int(r.integers(1,121)))); z[O[0]].append(dict(id=ids.get(O[0]),created_d=c,modified_d=m,version=i%4+1,application_d=ad,product_type_cd=None,application_type_cd=AT[i%7]))
 apps=frame(z[O[0]],O[0]); ac=cnt(n,2,r); seq=0
 for i,a in apps.iterrows():
  for j in range(ac[i]):
   c,m=aud(a.created_d,r); z[O[1]].append(dict(id=ids.get(O[1]),created_d=c,modified_d=m,version=seq%4+1,application_id=a.id,primary_applicant_flg=int(j==0),sms_marketing_opt_in_flg=seq%2,marketing_attribution_cd="DIRECT SEARCH PARTNER BRANCH".split()[seq%4])); seq+=1
 apl=frame(z[O[1]],O[1])
 for i,a in apps.iterrows():
  typ=a.application_type_cd; joint=int(isinstance(typ,str) and typ.startswith("JOINT_")); c,m=aud(a.created_d,r); common=dict(created_d=c,modified_d=m,version=i%4+1,application_id=a.id,source_cd="APPLICANT",is_joint_flg=joint)
  if typ in {AT[0],AT[3]}: t=O[2]; z[t].append(dict(id=ids.get(t),**common,initial_deposit_amount=f"{int(r.integers(25,25001))}.00",product_code="09 10 25".split()[len(z[t])%3],debit_card_requested_flg=i%2,account_type="SV"))
  elif typ in {AT[1],AT[5]}: t=O[3]; mn=int(r.integers(25,501)); ini=int(r.integers(mn,20001)); z[t].append(dict(id=ids.get(t),**common,debit_card_requested_flg=i%2,min_initial_deposit_amount=f"{mn}.00",initial_deposit_amount=f"{ini}.00",product_code="53 01".split()[len(z[t])%2],account_type="ND"))
  elif typ in {AT[2],AT[4]}: t=O[4]; z[t].append(dict(id=ids.get(t),**common,initial_deposit_amount=f"{int(r.integers(500,100001))}.00",product_code="14 26 25 22 06 12 21 23 28".split()[len(z[t])%9],apy=q(r.uniform(.5,6)),term_in_months=[3,6,9,12,18,24,36,48,60][len(z[t])%9],account_type="CD"))
 bands=[(580,619),(620,659),(660,699),(700,739),(740,799),(800,850)]
 for i,a in apl.iterrows():
  c,m=aud(a.created_d,r); first=f.first_name(); last=f.last_name(); dob=f.date_of_birth(minimum_age=21,maximum_age=80).strftime("%Y-%m-%d"); ssn=f"{int(r.integers(1e8,1e9)):09d}"; tax=int(r.integers(18000,180001)); nt=int(r.integers(0,20001)); lo,hi=bands[i%6]; t=O[5]; z[t].append(dict(id=ids.get(t),created_d=c,modified_d=m,version=i%4+1,applicant_id=a.id,source_cd="APPLICANT",first_name=first,first_name_keyn=first.lower(),first_name_keyv=i+1,last_name=last,last_name_keyn=last.lower(),last_name_keyv=i+1001,honorific_cd="MR MS MX DR".split()[i%4],date_of_birth=dob,date_of_birth_keyn=dob.replace("-",""),date_of_birth_keyv=int(dob.replace("-","")),ssn=ssn,ssn_keyn=ssn[-4:],ssn_keyv=int(ssn[-6:]),marital_status_cd="SINGLE MARRIED DIVORCED WIDOWED".split()[i%4],educational_qualification_cd="HIGH_SCHOOL BACHELOR MASTER DOCTORATE".split()[i%4],employment_status_cd=EMP[i%7],cumulative_job_tenure_months=str(int(r.integers(0,481))),annual_income=str(tax+nt),annual_additional_income=str(int(r.integers(0,30001))),annual_income_taxable=str(tax),annual_income_non_taxable=str(nt),housing_status_cd=HOUSE[i%7],annual_housing_expense=str(int(r.integers(0,60001))),annual_additional_debt=str(int(r.integers(0,40001))),fico_range_cd=f"{lo}-{hi}",fico_min=str(lo),fico_max=str(hi),citizenship=i%2)); c,m=aud(a.created_d,r); t=O[6]; em=f"{first}.{last}.{i}@example.test".lower().replace("'",""); z[t].append(dict(id=ids.get(t),created_d=c,modified_d=m,version=i%4+1,applicant_id=a.id,source_cd="APPLICANT",email_address=em,email_address_keyn=em,email_address_keyv=i+1))
 banks=sum(([x["id"] for x in z[t]] for t in O[2:5]),[]); joints=apps[apps.application_type_cd.astype("string").str.startswith("JOINT_",na=False)]; targets=apps[~apps.id.isin(joints.id)].id.tolist()
 for k,(j,a) in enumerate(joints.iterrows()):
  oi=ids.get(O[10]); oc,om=aud(a.created_d,r); z[O[10]].append(dict(id=oi,created_d=oc,modified_d=om,version=j%4+1,origination_guid=f.uuid4())); pi=ids.get(O[13]); pc,pm=aud(a.created_d,r); fl=tm(pc+pd.Timedelta(hours=1)); ll=tm(fl+pd.Timedelta(hours=j%48+1)); used=tm(ll+pd.Timedelta(hours=1)); z[O[13]].append(dict(id=pi,created_d=pc,modified_d=pm,version=j%4+1,source_cd="ONLINE BRANCH PARTNER".split()[j%3],credit_policy_name="STANDARD PREMIUM SECURED".split()[j%3],credit_policy_version=j%5+1,effective_start_d=pc,effective_end_d=tm(pc+pd.Timedelta(days=30+j%60)),status_cd="ACTIVE USED EXPIRED".split()[j%3],first_look_up=fl,last_look_up=ll,used_d=used,offer_amt=q(r.uniform(1000,50000)),offer_purpose_cd="DEPOSIT CROSS_SELL RETENTION".split()[j%3]))
  for ref,src,label in [(a.id,"11004","applicationid"),(pi,"11000","prescreenentry"),(banks[j%len(banks)],"11005","depositaccount")]: c,m=aud(oc,r); z[O[12]].append(dict(id=ids.get(O[12]),created_d=c,modified_d=m,version=j%4+1,origination_id=oi,ext_ref_id=ref,ext_src_id=src,ext_entity_name=label,flow_id=f"FLOW{j%11:03d}",flow_version=j%4+1))
  primary=targets[k%len(targets)]
  for x,direction in enumerate(["SOURCE","TARGET",None]): c,m=aud(pc,r); z[O[14]].append(dict(id=ids.get(O[14]),created_d=c,modified_d=m,version=(k+x)%4+1,ps_prescreen_entry_id=pi,ext_src_id="11004",ext_ref_id=primary if x==0 else targets[(k+x)%len(targets)],ext_entity_name="applicationid",ext_ref_direction_cd=direction))
 for i,a in apl.iterrows():
  oi=ids.get(O[10]); oc,om=aud(a.created_d,r); z[O[10]].append(dict(id=oi,created_d=oc,modified_d=om,version=i%4+1,origination_guid=f.uuid4())); c,m=aud(oc,r); z[O[12]].append(dict(id=ids.get(O[12]),created_d=c,modified_d=m,version=i%4+1,origination_id=oi,ext_ref_id=a.id,ext_src_id="11004",ext_entity_name="applicantid",flow_id=f"FLOW{i%11:03d}",flow_version=i%4+1)); c,m=aud(oc,r); z[O[11]].append(dict(id=ids.get(O[11]),created_d=c,modified_d=m,version=i%4+1,origination_id=oi,member_ref_id=f"ACT{a.id[-8:]}",member_src_id="1000 1200".split()[i%2],member_entity_name="loginactorid pfloginuserid".split()[i%2]))
 pseq=0; pcs=cnt(n,1.5,r)
 for i,a in apps.iterrows():
  for _ in range(pcs[i]): c,m=aud(a.created_d,r); sd=tm(c+pd.Timedelta(hours=2)); dd=tm(sd+pd.Timedelta(hours=2)); t=O[7]; z[t].append(dict(id=ids.get(t),created_d=c,modified_d=m,version=pseq%4+1,subject_ref_id=a.id,subject_entity_name="applicationid",type_cd=UWT[pseq%3],subject_src_id="11004",status_cd=UWS[pseq%7],product_type_cd="DEPOSIT BANKING RETAIL".split()[pseq%3],status_d=sd,decision_status_cd=DS[pseq%7],decision_status_d=dd,decision_phase_cd="INITIAL FULL FULFILLMENT".split()[pseq%3],subject_type_cd="PRIMARY JOINT INDIVIDUAL".split()[pseq%3],expiration_d=tm(c+pd.Timedelta(days=30+pseq%60)))); pseq+=1
 proc=frame(z[O[7]],O[7]); rseq=0; rcs=cnt(len(proc),2,r)
 for i,p in proc.iterrows():
  for _ in range(rcs[i]): c,m=aud(p.created_d,r); z[O[8]].append(dict(id=ids.get(O[8]),created_d=c,modified_d=m,version=rseq%4+1,uw_process_id=p.id,decision_phase_cd=PH[rseq%5],decision_status_cd=DS[rseq%7])); rseq+=1
 runs=frame(z[O[8]],O[8]); pools={"bankaccount":banks,"prescreenentry":[x["id"] for x in z[O[13]]],"applicantid":apl.id.tolist()}; eseq=0; ecs=cnt(len(runs),1.5,r)
 for i,run in runs.iterrows():
  for _ in range(ecs[i]):
   label="bankaccount prescreenentry applicantid".split()[eseq%3]; label=label if pools[label] else "applicantid"; c,m=aud(run.created_d,r); pool=pools[label]; z[O[9]].append(dict(id=ids.get(O[9]),created_d=c,modified_d=m,version=eseq%4+1,uw_process_run_id=run.id,ext_ref_id=pool[eseq%min(len(pool),10)],ext_src_id={"bankaccount":"11005","prescreenentry":"11000","applicantid":"11004"}[label],ext_ref_entity_name=label)); eseq+=1
 return norm({t:frame(z[t],t) for t in O})
def starts(d): return {t:int(d[t].id.astype("string").str.extract(r"(\d+)$",expand=False).astype(int).max()+1) for t in O}
def protected(d):
 ure=d[O[9]]; do=d[O[12]]; refs=set(ure.ext_ref_id)|set(do.loc[do.ext_entity_name.eq("depositaccount"),"ext_ref_id"]); out=set(d[O[1]].loc[d[O[1]].id.isin(refs),"application_id"])
 for t in O[2:5]: out|=set(d[t].loc[d[t].id.isin(refs),"application_id"])
 pres=set(ure.loc[ure.ext_ref_entity_name.eq("prescreenentry"),"ext_ref_id"]); org=set(do.loc[do.ext_entity_name.eq("prescreenentry")&do.ext_ref_id.isin(pres),"origination_id"]); out|=set(do.loc[do.origination_id.isin(org)&do.ext_entity_name.eq("applicationid"),"ext_ref_id"]); out|=set(d[O[14]].loc[d[O[14]].ext_ref_direction_cd.eq("SOURCE"),"ext_ref_id"]); return out
def incremental(existing,seed):
 r=np.random.default_rng(seed+7000); d={t:existing[t].copy().reset_index(drop=True) for t in O}; before={t:x.copy() for t,x in d.items()}; apps=d[O[0]]; nonjoint=~apps.application_type_cd.astype("string").str.startswith("JOINT_",na=False); candidates=apps.loc[nonjoint&~apps.id.isin(protected(d)),"id"].tolist(); n=max(1,round(len(apps)*DELETE_PCT/100)); assert len(candidates)>=n; aid=set(candidates[:n]); deleted={t:d[t].iloc[:0].copy() for t in O}
 def rem(t,mask): deleted[t]=d[t].loc[mask].copy(); d[t]=d[t].loc[~mask].reset_index(drop=True)
 apl=set(d[O[1]].loc[d[O[1]].application_id.isin(aid),"id"]); pro=set(d[O[7]].loc[d[O[7]].subject_ref_id.isin(aid),"id"]); run=set(d[O[8]].loc[d[O[8]].uw_process_id.isin(pro),"id"]); do=d[O[12]]; org=set(do.loc[(do.ext_entity_name.eq("applicationid")&do.ext_ref_id.isin(aid))|(do.ext_entity_name.eq("applicantid")&do.ext_ref_id.isin(apl)),"origination_id"]); pre=set(do.loc[do.origination_id.isin(org)&do.ext_entity_name.eq("prescreenentry"),"ext_ref_id"])
 rem(O[9],d[O[9]].uw_process_run_id.isin(run)); rem(O[8],d[O[8]].id.isin(run)); rem(O[7],d[O[7]].id.isin(pro)); rem(O[5],d[O[5]].applicant_id.isin(apl)); rem(O[6],d[O[6]].applicant_id.isin(apl)); rem(O[11],d[O[11]].origination_id.isin(org)); rem(O[14],d[O[14]].ps_prescreen_entry_id.isin(pre)); rem(O[12],d[O[12]].origination_id.isin(org)); rem(O[13],d[O[13]].id.isin(pre)); rem(O[10],d[O[10]].id.isin(org))
 for t in O[2:5]: rem(t,d[t].application_id.isin(aid))
 rem(O[1],d[O[1]].id.isin(apl)); rem(O[0],d[O[0]].id.isin(aid)); updated={}
 for t in O:
  k=max(1,round(len(d[t])*UPDATE_PCT/100)) if len(d[t]) else 0; ix=np.sort(r.choice(d[t].index,size=k,replace=False)) if k else []
  if k: d[t].loc[ix,"version"]=pd.to_numeric(d[t].loc[ix,"version"])+1; d[t].loc[ix,"modified_d"]=pd.to_datetime(d[t].loc[ix,"modified_d"])+pd.Timedelta(days=1)
  updated[t]=d[t].loc[ix].copy()
 ins=generate(max(1,round(len(before[O[0]])*INSERT_PCT/100)),seed+9000,starts(before)); changes={}
 for t in O:
  d[t]=pd.concat([d[t],ins[t]],ignore_index=True); parts=[]
  for x,op in [(deleted[t],"DELETE"),(updated[t],"UPDATE"),(ins[t],"INSERT")]: y=x.copy(); y["_operation"]=op; parts.append(y)
  changes[t]=pd.concat(parts,ignore_index=True)
 return norm(d),changes,before
def read_existing(bucket,prefix,local=None):
 out={}; s3=None if local else boto3.client("s3")
 for t in O:
  sc,na=t.split("."); src=os.path.join(local,sc,na+".csv") if local else io.BytesIO(s3.get_object(Bucket=bucket,Key=f"{prefix.strip('/')}/{sc}/{na}.csv")["Body"].read()); dtype={}; conv={}; dates=[]
  for c,y in zip(C[t],T[t]):
   if y=="s": dtype[c]="string"
   elif y=="i": dtype[c]="Int64"
   elif y=="d": conv[c]=lambda v:q(v) if v!="" else None
   else: dates.append(c)
  x=pd.read_csv(src,dtype=dtype,converters=conv,parse_dates=dates)
  if "_operation" in x: x=x.drop(columns="_operation")
  out[t]=x[C[t]]
 return norm(out)
def local_write(d,path):
 for t,x in d.items(): sc,na=t.split("."); os.makedirs(os.path.join(path,sc),exist_ok=True); x.to_csv(os.path.join(path,sc,na+".csv"),index=False,date_format="%Y-%m-%d %H:%M:%S.%f")
def ck(a,label,p): assert bool(p),label; a.append(label)
def validate(d,label):
 a=[]; ck(a,"literal table set",set(d)==set(C))
 for t in O:
  x=d[t]; ck(a,t+" literal columns",list(x)==C[t]); ck(a,t+" PK",x.id.notna().all() and x.id.is_unique); ck(a,t+" audit order",(x.modified_d>=x.created_d).all())
  for c,y in zip(C[t],T[t]):
   v=x[c].dropna()
   if y=="s": ck(a,t+"."+c+" strings",v.map(lambda z:isinstance(z,str)).all())
   elif y=="d": ck(a,t+"."+c+" Decimal scale",v.map(lambda z:isinstance(z,Decimal) and z.as_tuple().exponent==-2).all())
   elif y=="t": ck(a,t+"."+c+" datetime",pd.api.types.is_datetime64_any_dtype(x[c]))
   else: ck(a,t+"."+c+" integral",(pd.to_numeric(v)%1==0).all())
  ck(a,t+" null semantics",not x.select_dtypes(include=["object","string"]).apply(lambda s:s.dropna().astype(str).isin({"None","nan","NULL"}).any()).any())
 for (t,c),dom in ENUM.items():
  obs=set(d[t][c].dropna().astype(str)); allowed={v for v in dom if v is not None}; ck(a,t+"."+c+" enum",obs<=allowed); ck(a,t+"."+c+" coverage",obs==allowed-EX.get((t,c),set()))
  if None in dom: ck(a,t+"."+c+" SQL NULL",d[t][c].isna().any())
 apps=d[O[0]]; apl=d[O[1]]; A=set(apps.id); B=set(apl.id); g=apl.groupby("application_id"); ck(a,"applicant relationship",set(g.groups)==A and g.size().between(1,3).all()); ck(a,"one primary",g.primary_applicant_flg.sum().eq(1).all())
 for t,types in [(O[2],{AT[0],AT[3]}),(O[3],{AT[1],AT[5]}),(O[4],{AT[2],AT[4]})]: ck(a,t+" conditional 1:1",set(d[t].application_id)==set(apps.loc[apps.application_type_cd.isin(types),"id"]) and d[t].application_id.is_unique)
 ck(a,"checking amounts",all(Decimal(x)<=Decimal(y) for x,y in zip(d[O[3]].min_initial_deposit_amount,d[O[3]].initial_deposit_amount)))
 for t in [O[5],O[6]]: ck(a,t+" applicant coverage",set(d[t].applicant_id)==B and d[t].applicant_id.is_unique)
 det=d[O[5]]; ck(a,"FICO",all(int(x)<=int(y) and z==f"{x}-{y}" for x,y,z in zip(det.fico_min,det.fico_max,det.fico_range_cd))); ck(a,"income",all(int(x)==int(y)+int(z) for x,y,z in zip(det.annual_income,det.annual_income_taxable,det.annual_income_non_taxable)))
 pro=d[O[7]]; pc=pro.groupby("subject_ref_id").size(); ck(a,"UW process relationship",set(pc.index)==A and pc.between(1,2).all()); ck(a,"UW times",((pro.status_d>=pro.created_d)&(pro.decision_status_d>=pro.status_d)&(pro.expiration_d>=pro.created_d)).all()); runs=d[O[8]]; rc=runs.groupby("uw_process_id").size(); ck(a,"runs relationship",set(rc.index)==set(pro.id) and rc.between(1,3).all()); ure=d[O[9]]; ec=ure.groupby("uw_process_run_id").size(); ck(a,"run refs relationship",set(ec.index)==set(runs.id) and ec.between(1,2).all())
 banks=set().union(*(set(d[t].id) for t in O[2:5])); pres=set(d[O[13]].id)
 for k,pool in {"bankaccount":banks,"prescreenentry":pres,"applicantid":B}.items(): ck(a,"UW semantic "+k,set(ure.loc[ure.ext_ref_entity_name.eq(k),"ext_ref_id"])<=pool)
 do=d[O[12]]; orig=set(d[O[10]].id); mem=d[O[11]]; ck(a,"DAS FKs",set(do.origination_id)<=orig and set(mem.origination_id)<=orig)
 for k,pool in {"applicationid":A,"prescreenentry":pres,"applicantid":B,"depositaccount":banks}.items(): ck(a,"DAS semantic "+k,set(do.loc[do.ext_entity_name.eq(k),"ext_ref_id"])<=pool)
 al=do[do.ext_entity_name.eq("applicantid")]; ck(a,"applicant DAS coverage",set(al.ext_ref_id)==B and al.ext_ref_id.is_unique); ck(a,"member chain",set(mem.origination_id)==set(al.origination_id) and mem.origination_id.is_unique and mem.member_ref_id.is_unique)
 joint=set(apps.loc[apps.application_type_cd.astype("string").str.startswith("JOINT_",na=False),"id"]); d1=do[do.ext_src_id.eq("11004")&do.ext_entity_name.eq("applicationid")]; d2=do[do.ext_src_id.eq("11000")&do.ext_entity_name.eq("prescreenentry")]; per=d[O[14]]; src=per[per.ext_src_id.eq("11004")&per.ext_entity_name.eq("applicationid")&per.ext_ref_direction_cd.eq("SOURCE")]; ck(a,"joint doer1",set(d1.ext_ref_id)==joint and d1.ext_ref_id.is_unique); ck(a,"paired doer2",set(d2.origination_id)==set(d1.origination_id) and set(d2.ext_ref_id)==pres); ck(a,"prescreen ext relationship",set(per.ps_prescreen_entry_id)==pres and per.groupby("ps_prescreen_entry_id").size().eq(3).all()); ck(a,"SOURCE mapping",set(src.ps_prescreen_entry_id)==pres and src.ps_prescreen_entry_id.is_unique and set(src.ext_ref_id)<=A-joint)
 chain=d1[["ext_ref_id","origination_id"]].merge(d2,on="origination_id",suffixes=("_joint","_pre")).merge(src,left_on="ext_ref_id_pre",right_on="ps_prescreen_entry_id"); ck(a,"complete distinct chain",len(chain)==len(joint) and (chain.ext_ref_id_joint!=chain.ext_ref_id).all() and (len(chain)<2 or chain.ext_ref_id.nunique()>1)); pe=d[O[13]]; ck(a,"prescreen times",((pe.effective_end_d>=pe.effective_start_d)&(pe.last_look_up>=pe.first_look_up)&(pe.used_d>=pe.last_look_up)).all())
 print("DISTINCT VALUE REPORT",label)
 for t in O:
  x=d[t]; print(t,", ".join(f"{c}={x[c].nunique(dropna=False)}" for c in C[t]))
  if len(x)>1:
   for c in C[t]:
    if (t,c) not in PIN: ck(a,t+"."+c+" diverse",x[c].nunique(dropna=False)>1)
 print(f"VALIDATION PASSED ({label}): {len(a)} asserted checks"); return True
def selftest():
 print("STARTING INCREMENTAL SELF-TEST"); base=generate(SELF_TEST_APPLICATION_ROWS,SEED+3000); validate(base,"self-test raw")
 with tempfile.TemporaryDirectory() as p: local_write(base,p); loaded=read_existing("","",p); merged,ch,before=incremental(loaded,SEED+4000)
 validate(merged,"self-test incremental"); print("INCREMENTAL OPERATION COUNTS")
 for t in O:
  vc=ch[t]._operation.value_counts(); vals=[int(vc.get(x,0)) for x in ["INSERT","UPDATE","DELETE"]]; print(t,dict(zip(["inserted","updated","deleted"],vals)))
  for got,pct in zip(vals,[INSERT_PCT,UPDATE_PCT,DELETE_PCT]): assert abs(got-len(before[t])*pct/100)<=max(4,len(before[t])*pct/100*.70)
  assert set(ch[t].loc[ch[t]._operation.eq("INSERT"),"id"]).isdisjoint(set(before[t].id)); assert ch[t].groupby("_operation").id.apply(lambda s:s.is_unique).all()
 assert not merged[O[0]].equals(generate(len(merged[O[0]]),SEED+3000)[O[0]]); print("INCREMENTAL SELF-TEST PASSED")
def upload(d):
 s3=boto3.client("s3"); clean=S3_PREFIX.strip("/"); sc,na=O[0].split("."); first=f"{clean}/{sc}/{na}.csv"; print(f"First constructed S3 key: s3://{S3_BUCKET}/{first}"); assert "//" not in f"s3://{S3_BUCKET}/{first}".replace("s3://","")
 for t,x in d.items(): sc,na=t.split("."); key=f"{clean}/{sc}/{na}.csv"; s3.put_object(Bucket=S3_BUCKET,Key=key,Body=x.to_csv(index=False,date_format="%Y-%m-%d %H:%M:%S.%f").encode(),ContentType="text/csv"); print(f"Uploaded s3://{S3_BUCKET}/{key}")
def main():
 random.seed(SEED); Faker.seed(SEED); fake.seed_instance(SEED); assert OUTPUT_FORMAT=="csv"; selftest()
 if MODE=="full": data=generate(max(1,round(ROOT_ROW_COUNTS[O[0]]*SCALE_FACTOR)),SEED); out=data
 elif MODE=="incremental": data,ch,_=incremental(read_existing(S3_BUCKET,S3_PREFIX),SEED); out=data if INCREMENTAL_OUTPUT=="merged" else ch
 else: raise ValueError("MODE")
 validate(data,"production "+MODE)
 if UPLOAD_ENABLED: upload(out)
 else: print("UPLOAD SKIPPED (dev/test run)")
 print("RUN SUMMARY")
 for t,x in out.items(): print(t,len(x),"rows",len(x.columns),"columns")
 return out
TABLES=main()
