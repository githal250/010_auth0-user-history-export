#----------------------
## organization側のprm_expire_date?の取得に挑戦
#----------------------

from dotenv import load_dotenv
load_dotenv()
import os, requests
DOMAIN=os.getenv("AUTH0_DOMAIN");CID=os.getenv("AUTH0_CLIENT_ID");CSEC=os.getenv("AUTH0_CLIENT_SECRET")

# トークン取得
treq={"client_id":CID,"client_secret":CSEC,"audience":f"https://{DOMAIN}/api/v2/","grant_type":"client_credentials"}
tok=requests.post(f"https://{DOMAIN}/oauth/token",json=treq).json().get("access_token")
hdr={"Authorization":f"Bearer {tok}"}

def get_prm_expire_date(member):
    if "prm_expire_date" in member: return member["prm_expire_date"]
    for k in ("app_metadata","user_metadata","metadata"):
        if isinstance(member.get(k),dict) and "prm_expire_date" in member[k]:
            return member[k]["prm_expire_date"]
    return ""

# 組織一覧（ページ処理）
orgs_r = requests.get(f"https://{DOMAIN}/api/v2/organizations", headers=hdr, params={"per_page":50}).json()
for org in orgs_r:
    page=0
    while True:
        r = requests.get(f"https://{DOMAIN}/api/v2/organizations/{org['id']}/members",
                         headers=hdr, params={"page":page,"per_page":50})
        if r.status_code!=200:
            print("ERR", org["id"], r.status_code, r.text); break
        members = r.json()
        if not members: break
        for m in members:
            print(org["name"], m.get("user_id") or m.get("id"), get_prm_expire_date(m))
        page+=1