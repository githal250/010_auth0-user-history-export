#########################################################
# DEN-UPのユーザ一覧をAuth0から取得してexcel出力する
# さらにOrganizationの最新 prm_expire_date をユーザー一覧へ反映する
#########################################################

import requests
import json
from datetime import datetime, timezone, timedelta
import os
import sys
from dotenv import load_dotenv
import pandas as pd
import traceback
from typing import Any, Optional, Callable

from organization import export_organization_list

def build_org_created_map(users):
    m={}
    for u in users:
        c=u.get("app_metadata",{}).get("organization_data",{}).get("metadata",{}).get("company_name","")
        d=u.get("created_at","")
        if c and d and (c not in m or d<m[c]): m[c]=d
    return m

def apply_created_at_to_org_rows(org_rows, users):
    m=build_org_created_map(users)
    for r in org_rows:
        c=r.get("organization","")
        d=m.get(c,"")
        r["created_at_raw"]=d
        r["created_at"]=_normalize_datetime(d) if d else ""
    return org_rows

# # ★追加
# def build_org_created_map(users):
#     org_created = {}
#     for u in users:
#         company = u.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_name", "")
#         created = u.get("created_at", "")
#         if not company or not created:
#             continue
#         if company not in org_created or created < org_created[company]:
#             org_created[company] = created
#     return org_created

def find_prm_expire(obj):
    if isinstance(obj, dict):
        if "prm_expire_date" in obj:
            return obj["prm_expire_date"]
        for v in obj.values():
            r = find_prm_expire(v)
            if r is not None:
                return r
    if isinstance(obj, list):
        for i in obj:
            r = find_prm_expire(i)
            if r is not None:
                return r
    return None


def _script_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(sys.argv[0]))


script_dir = _script_dir()
env_path = os.path.join(script_dir, ".env")
load_dotenv(dotenv_path=env_path)

domain = os.getenv("AUTH0_DOMAIN")
client_id = os.getenv("AUTH0_CLIENT_ID")
client_secret = os.getenv("AUTH0_CLIENT_SECRET")
audience = f"https://{domain}/api/v2/"


def get_access_token(domain, client_id, client_secret, audience):
    token_url = f"https://{domain}/oauth/token"
    headers = {"Content-Type": "application/json"}
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience
    }
    response = requests.post(token_url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def get_users_by_query(query, domain, access_token, per_page=100):
    users = []
    page = 0
    url = f"https://{domain}/api/v2/users"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "per_page": per_page,
        "page": page,
        "q": query,
        "search_engine": "v3"
    }
    while True:
        params["page"] = page
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        users.extend(data)
        if len(data) < per_page:
            break
        page += 1
    return users


def get_all_users_segmented(domain, access_token, per_page=100):
    all_users = []
    prefixes = [chr(i) for i in range(ord("A"), ord("Z") + 1)] + [str(i) for i in range(0, 10)]
    for prefix in prefixes:
        query = f"email:{prefix}*"
        print(f"クエリ '{query}' でユーザー取得中...")
        users_segment = get_users_by_query(query, domain, access_token, per_page)
        print(f"  {len(users_segment)}件取得")
        all_users.extend(users_segment)

    try:
        print("クエリ 'NOT email:*' でメール無しユーザー取得中...")
        users_no_email = get_users_by_query("NOT email:*", domain, access_token, per_page)
        print(f"  {len(users_no_email)}件取得")
        all_users.extend(users_no_email)
    except requests.HTTPError as e:
        print("メール無しユーザーの取得でエラー発生（スキップ）:", e)

    unique_users = {user.get("user_id"): user for user in all_users if user.get("user_id")}
    return list(unique_users.values())


JST = timezone(timedelta(hours=9))


def _normalize_datetime(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        iv = int(value)
        if iv > 10**12:
            iv = iv / 1000.0
        try:
            dt = datetime.fromtimestamp(iv, timezone.utc).astimezone(JST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit() and len(s) == 8:
            try:
                yyyy = int(s[0:4]); mm = int(s[4:6]); dd = int(s[6:8])
                dt = datetime(yyyy, mm, dd, 0, 0, 0, tzinfo=JST)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=JST)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(JST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        try:
            iv = int(s)
            if iv > 10**12:
                iv = iv / 1000.0
            dt = datetime.fromtimestamp(iv, timezone.utc).astimezone(JST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return ""


def _norm_key(k: str) -> str:
    return k.lower().replace(" ", "").replace("_", "").replace("-", "")


def _find_key_recursive(obj: Any, target_names):
    targets = {_norm_key(t) for t in target_names}

    def _rec(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if _norm_key(k) in targets:
                    return v
                res = _rec(v)
                if res is not None:
                    return res
        elif isinstance(o, list):
            for item in o:
                res = _rec(item)
                if res is not None:
                    return res
        return None

    return _rec(obj)


def _build_org_prm_map(org_rows):
    m = {}
    for r in org_rows or []:
        org_name = str(r.get("organization", "") or "")
        raw = r.get("prm_expire_date_raw", "") or r.get("prm_expire_date", "") or ""
        if org_name:
            m[_norm_key(org_name)] = raw
    return m


# ★変更
def rename_and_flatten_fields(users, org_rows=None, org_created_map=None):
    org_prm_map = _build_org_prm_map(org_rows)
    new_users = []

    for user in users:
        company_name = user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_name", "")

        new_user = {
            "user_id": user.get("user_id", ""),
            "email": user.get("email", ""),
            "Email Verified": user.get("email_verified", False),
            "updated_at": _normalize_datetime(user.get("updated_at", "")),
            "Column1.user_metadata.last_name": user.get("user_metadata", {}).get("last_name", ""),
            "Column1.user_metadata.first_name": user.get("user_metadata", {}).get("first_name", ""),
            "Column1.app_metadata.organization_data.metadata.company_address.postcode":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_address", {}).get("postcode", ""),
            "Column1.app_metadata.organization_data.metadata.company_address.prefecture":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_address", {}).get("prefecture", ""),
            "Column1.app_metadata.organization_data.metadata.company_address.city":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_address", {}).get("city", ""),
            "Column1.app_metadata.organization_data.metadata.company_name": company_name
        }

        org_created = org_created_map.get(company_name, "") if org_created_map else ""
        new_user["Column1.org_created_at_raw"] = org_created
        new_user["Column1.会社作成日"] = _normalize_datetime(org_created) if org_created else ""

        found_prm = org_prm_map.get(_norm_key(company_name), "")
        if not found_prm:
            found_prm = user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("prm_expire_date")
        if not found_prm:
            found_prm = find_prm_expire(user)

        new_user["Column1.prm_expire_date_raw"] = found_prm if found_prm else ""
        new_user["Column1.解約予定日"] = _normalize_datetime(found_prm) if found_prm else ""

        sf = user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("salesforce_id")
        if not sf:
            sf = _find_key_recursive(user, ["salesforce_id", "salesforceid"])
        new_user["salesforce_id"] = sf or ""

        cands = ["expire_date", "last_login", "last_login_at", "last_login_date", "lastlogindate", "login_at"]
        found_expire = _find_key_recursive(user, cands)
        new_user["Column1.expire_date_raw"] = found_expire or ""
        new_user["Column1.最終ログイン日"] = _normalize_datetime(found_expire) if found_expire else ""

        created_raw = user.get("created_at", "") or _find_key_recursive(user, ["created_at"])
        new_user["Column1.created_at_raw"] = created_raw or ""
        new_user["Column1.アカウント作成日"] = _normalize_datetime(created_raw) if created_raw else ""

        new_users.append(new_user)

    return new_users


def export_all_data(domain, client_id, client_secret, audience, per_page=100, progress_callback: Optional[Callable[[str], None]] = None):
    try:
        def progress(msg: str):
            print(msg)
            if progress_callback:
                progress_callback(msg)

        progress("Auth0にアクセスしています")
        access_token = get_access_token(domain, client_id, client_secret, audience)
        progress("Auth0への接続が完了しました")

        progress("会社リストを作成しています")
        org_ok, org_info = export_organization_list(domain, client_id, client_secret, audience, per_page=100)
        if not org_ok:
            raise RuntimeError(f"Organization出力に失敗: {org_info}")
        org_rows = org_info.get("rows", []) if isinstance(org_info, dict) else []

        progress("ユーザーリストを作成しています")
        raw_users = get_all_users_segmented(domain, access_token, per_page=per_page)
        total_count = len(raw_users)
        progress(f"ユーザー取得完了: {total_count}件")

        progress("ユーザー情報を整形しています")
        org_created_map = build_org_created_map(raw_users) 
        users = rename_and_flatten_fields(raw_users, org_rows=org_rows, org_created_map=org_created_map)

        org_rows = apply_created_at_to_org_rows(org_rows, raw_users)
        pd.DataFrame(org_rows).to_csv(org_info["csv"], index=False, encoding="utf-8-sig")
        pd.DataFrame(org_rows).to_excel(org_info["excel"], index=False)


        output_dir = os.path.join(script_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_filename = f"userlist_{current_time}_件数{total_count}"
        json_filename = os.path.join(output_dir, f"{base_filename}.json")
        excel_filename = os.path.join(output_dir, f"{base_filename}.xlsx")

        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        progress("JSONを保存しました")

        df = pd.json_normalize(users)
        df.to_excel(excel_filename, index=False)
        progress("Excelを保存しました")

        return True, {
            "json": json_filename,
            "excel": excel_filename,
            "organization_csv": org_info.get("csv", "") if isinstance(org_info, dict) else "",
            "organization_excel": org_info.get("excel", "") if isinstance(org_info, dict) else "",
            "organization_rows": org_rows,
        }

    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"{e}\n{tb}"
        print("エクスポート中にエラーが発生しました:", err_msg)
        return False, err_msg


def run_export(progress_callback: Optional[Callable[[str], None]] = None):
    try:
        ok, info = export_all_data(domain, client_id, client_secret, audience, per_page=100, progress_callback=progress_callback)
        if ok:
            return True, info.get("excel", info)
        else:
            return False, info
    except Exception as e:
        tb = traceback.format_exc()
        return False, f"{e}\n{tb}"


def main():
    ok, info = export_all_data(domain, client_id, client_secret, audience, per_page=100)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()