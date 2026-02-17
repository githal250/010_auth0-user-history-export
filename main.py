#########################################################
# DEN-UPのユーザ一覧をAuth0から取得してexcel出力する 
#########################################################

import requests
import json
from datetime import datetime
import os
import sys
from dotenv import load_dotenv
import pandas as pd

#--------------------------------------------------
# .env ファイルの読み込み（スクリプトと同じディレクトリ）
#--------------------------------------------------
if getattr(sys, 'frozen', False):
    # exeとして実行されている場合
    script_dir = os.path.dirname(sys.executable)
else:
    # 通常のPythonスクリプトとして実行されている場合
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

env_path = os.path.join(script_dir, ".env")
load_dotenv(dotenv_path=env_path)

domain = os.getenv("AUTH0_DOMAIN")
client_id = os.getenv("AUTH0_CLIENT_ID")
client_secret = os.getenv("AUTH0_CLIENT_SECRET")
audience = f"https://{domain}/api/v2/"

#--------------------------------------------------
# アクセストークン取得
#--------------------------------------------------
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
    token_data = response.json()
    return token_data["access_token"]

#--------------------------------------------------
# 指定したクエリ条件(q)でユーザー取得（ページネーション対応）
#--------------------------------------------------
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

#--------------------------------------------------
# セグメント別にユーザー情報を取得して全件統合する関数
#--------------------------------------------------
def get_all_users_segmented(domain, access_token, per_page=100):
    all_users = []
    prefixes = [chr(i) for i in range(ord('A'), ord('Z') + 1)] + [str(i) for i in range(0, 10)]
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
    unique_users = {user.get('user_id'): user for user in all_users if user.get('user_id')}
    return list(unique_users.values())

#--------------------------------------------------
# 取得ユーザー情報から必要項目を平坦化＆項目名を変更する関数
#--------------------------------------------------
def rename_and_flatten_fields(users):
    new_users = []
    for user in users:
        new_user = {
            "user_id": user.get("user_id", ""),
            "email": user.get("email", ""),
            "Email Verified": user.get("email_verified", ""),
            "created_at": user.get("created_at", ""),
            "updated_at": user.get("updated_at", ""),
            "Column1.user_metadata.last_name": user.get("user_metadata", {}).get("last_name", ""),
            "Column1.user_metadata.first_name": user.get("user_metadata", {}).get("first_name", "")
        }
        app_metadata = user.get("app_metadata", {})
        org_data = app_metadata.get("organization_data", {})
        metadata = org_data.get("metadata", {})
        company_address = metadata.get("company_address", {})
        new_user["Column1.app_metadata.organization_data.metadata.company_address.postcode"] = company_address.get("postcode", "")
        new_user["Column1.app_metadata.organization_data.metadata.company_address.prefecture"] = company_address.get("prefecture", "")
        new_user["Column1.app_metadata.organization_data.metadata.company_address.city"] = company_address.get("city", "")
        new_user["Column1.app_metadata.organization_data.metadata.company_name"] = metadata.get("company_name", "")
        new_users.append(new_user)
    return new_users

#--------------------------------------------------
# メイン処理
#--------------------------------------------------
def main():
    print("アクセストークンを取得中...")
    access_token = get_access_token(domain, client_id, client_secret, audience)
    print("アクセストークン取得完了。")

    print("すべてのユーザー情報をセグメント別に取得中...")
    users = get_all_users_segmented(domain, access_token, per_page=100)
    total_count = len(users)
    print(f"全体の取得件数: {total_count}")

    print("ユーザー情報の整形（ネスト解除＆項目名変更）を行っています...")
    users = rename_and_flatten_fields(users)

    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_filename = f"userlist_{current_time}_件数{total_count}"
    json_filename = os.path.join(output_dir, f"{base_filename}.json")
    excel_filename = os.path.join(output_dir, f"{base_filename}.xlsx")

    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)
    print(f"すべてのユーザーデータを {json_filename} に出力しました。")

    df = pd.json_normalize(users)
    df.to_excel(excel_filename, index=False)
    print(f"変換完了：{excel_filename} に保存されました。")

if __name__ == "__main__":
    main()
