from dotenv import load_dotenv
import os
import sys
import traceback
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

JST = timezone(timedelta(hours=9))

def _script_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(sys.argv[0]))

def _normalize_datetime(value):
    if not value:
        return ""
    try:
        if isinstance(value, (int, float)):
            if value > 10**12:
                value /= 1000
            return datetime.fromtimestamp(value, timezone.utc).astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def get_access_token(domain, client_id, client_secret, audience):
    url = f"https://{domain}/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
    }
    r = requests.post(url, json=payload)
    r.raise_for_status()
    return r.json()["access_token"]

def get_all_organizations(domain, access_token, per_page=100):
    url = f"https://{domain}/api/v2/organizations"
    headers = {"Authorization": f"Bearer {access_token}"}
    page = 0
    results = []

    while True:
        r = requests.get(url, headers=headers, params={"page": page, "per_page": per_page})
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        results.extend(data)
        if len(data) < per_page:
            break
        page += 1

    return results

def get_organization_detail(domain, access_token, org_id):
    url = f"https://{domain}/api/v2/organizations/{org_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

def extract_prm_expire_date(org_detail):
    for src in [
        org_detail.get("metadata", {}),
        org_detail.get("organization_data", {}).get("metadata", {}),
    ]:
        if isinstance(src, dict) and "prm_expire_date" in src:
            return src.get("prm_expire_date")
    return ""

def build_org_rows(domain, access_token, per_page=100):
    rows = []
    orgs = get_all_organizations(domain, access_token, per_page)

    for org in orgs:
        org_id = org.get("id", "")

        try:
            detail = get_organization_detail(domain, access_token, org_id) if org_id else org
        except Exception:
            detail = org

        jp_name = (
            detail.get("metadata", {}).get("company_name")
            or detail.get("organization_data", {}).get("metadata", {}).get("company_name")
        )
        org_name = jp_name if jp_name else org.get("name", "")

        prm_raw = extract_prm_expire_date(detail)

        rows.append({
            "organization_id": org_id,
            "organization": org_name,
            "prm_expire_date_raw": prm_raw or "",
            "prm_expire_date": _normalize_datetime(prm_raw) if prm_raw else "",
        })

    return rows

def export_organization_list(domain, client_id, client_secret, audience, per_page=100):
    try:
        access_token = get_access_token(domain, client_id, client_secret, audience)
        rows = build_org_rows(domain, access_token, per_page)

        output_dir = os.path.join(_script_dir(), "output")
        os.makedirs(output_dir, exist_ok=True)

        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_filename = f"orglist_{current_time}_件数{len(rows)}"

        csv_path = os.path.join(output_dir, f"{base_filename}.csv")
        xlsx_path = os.path.join(output_dir, f"{base_filename}.xlsx")

        df = pd.DataFrame(rows)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        df.to_excel(xlsx_path, index=False)

        return True, {
            "csv": csv_path,
            "excel": xlsx_path,
            "rows": rows,
        }

    except Exception as e:
        return False, f"{e}\n{traceback.format_exc()}"

def main():
    script_dir = _script_dir()
    load_dotenv(os.path.join(script_dir, ".env"))

    domain = os.getenv("AUTH0_DOMAIN")
    client_id = os.getenv("AUTH0_CLIENT_ID")
    client_secret = os.getenv("AUTH0_CLIENT_SECRET")
    audience = f"https://{domain}/api/v2/"

    ok, info = export_organization_list(domain, client_id, client_secret, audience)
    if not ok:
        print(info)
        sys.exit(1)

    print(f"organizations={len(info['rows'])} written")

if __name__ == "__main__":
    main()