import csv, random, os
from datetime import datetime, timedelta
REGIONS=["DE-HE","DE-BY","DE-NW","DE-BW","DE-NI","DE-SN","DE-RP","DE-BB","DE-TH","DE-ST","DE-MV","DE-SH","DE-HB","DE-SL","DE-HH","DE-BE"]
CAMPAIGNS=["credit_card_offer","personal_loan","mortgage_offer","savings_account","investment_product","insurance_offer","refinancing_offer","overdraft_offer"]
STATUSES=["answered","no_answer","busy","voicemail","callback_requested"]
RESPONSES=["interested","not_interested","callback_requested","already_have_product","will_consider","transferred_to_agent","no_response","complaint"]
start=datetime(2026,1,1); delta=(datetime(2026,5,31)-start).days
with open("/root/etl-exam/data/transactions_v2.csv","w",newline="") as f:
    w=csv.writer(f)
    w.writerow(["call_id","call_time","client_id","region_code","campaign_type","call_status","client_response","duration_sec","follow_up_required"])
    for i in range(1,200001):
        d=start+timedelta(days=random.randint(0,delta))
        t=d+timedelta(hours=random.randint(8,20),minutes=random.randint(0,59),seconds=random.randint(0,59))
        s=random.choice(STATUSES)
        r=random.choice(RESPONSES) if s=="answered" else "no_response"
        dur=random.randint(30,900) if s=="answered" else random.randint(5,30)
        w.writerow([f"call_{t.strftime('%Y%m%d')}_{i:06d}",t.strftime("%Y-%m-%d %H:%M:%S"),f"client_{random.randint(1000,99999)}",random.choice(REGIONS),random.choice(CAMPAIGNS),s,r,dur,random.choice(["true","false"])])
        if i%50000==0: print(f"  {i:,}...")
print(f"✅ {os.path.getsize('/root/etl-exam/data/transactions_v2.csv')/1024/1024:.1f} МБ")