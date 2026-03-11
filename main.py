#########################################################
# DEN-UPのユーザ一覧をAuth0から取得してexcel出力する 
#########################################################

import requests
import json
from datetime import datetime, timezone, timedelta
import os
import sys
from dotenv import load_dotenv
import pandas as pd
import traceback
from typing import Any

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
# 取得ユーザー情報から必要項目を平坦化＆項目名を変更する関数　※2026.2.17書き換え
#--------------------------------------------------

# JST（出力は日本時間に統一）
JST = timezone(timedelta(hours=9))

def _normalize_datetime(value: Any) -> str:
    """
    - 入力: ISO, 'YYYYMMDD', 'YYYY/MM/DD', UNIX秒/ミリ秒 (int/str) 等を想定
    - 出力: 'YYYY-MM-DD HH:MM:SS'（JST）
    - 失敗時は空文字
    """
    if value is None or value == "":
        return ""
    # 数値型
    if isinstance(value, (int, float)):
        iv = int(value)
        if iv > 10**12:
            iv = iv / 1000.0
        try:
            dt = datetime.fromtimestamp(iv, timezone.utc).astimezone(JST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""
    # 文字列
    if isinstance(value, str):
        s = value.strip()
        # 1) YYYYMMDD (例: 20260217)
        if s.isdigit() and len(s) == 8:
            try:
                yyyy = int(s[0:4]); mm = int(s[4:6]); dd = int(s[6:8])
                dt = datetime(yyyy, mm, dd, 0, 0, 0, tzinfo=JST)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        # 2) YYYY/MM/DD や YYYY-MM-DD だけの形式
        for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=JST)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        # 3) ISO形式 (Z付き含む)
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(JST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        # 4) 数字文字列（UNIX秒/ミリ秒）
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
    """キーの正規化（小文字・空白とアンダースコアを削る）"""
    return k.lower().replace(" ", "").replace("_", "")

def _find_key_recursive(obj: Any, target_names):
    """
    ネストした dict/list を探索して、キー名が target_names のいずれかとマッチする最初の値を返す。
    比較は_norm_keyで正規化して行う。
    """
    targets = { _norm_key(t) for t in target_names }

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

def rename_and_flatten_fields(users):
    """
    users: list of user dicts (Auth0 エクスポート形式想定)
    戻り: new_users: list of dicts（出力用）
    マスト項目（あなたの要件）:
      - created_at, updated_at, Email Verified, 氏名, 住所（postcode/prefecture/city）, company_name
    さらに 解約予定日 を Column1.prm_expire_date_raw / Column1.prm_expire_date として追加
    """
    new_users = []
    for user in users:
        # --- 既存マストフィールドを必ず残す ---
        new_user = {
            "user_id": user.get("user_id", ""),
            "email": user.get("email", ""),
            "Email Verified": user.get("email_verified", False),
            "created_at": _normalize_datetime(user.get("created_at", "")),
            "updated_at": _normalize_datetime(user.get("updated_at", "")),

            # 名前（user_metadata 由来を想定）
            "Column1.user_metadata.last_name": user.get("user_metadata", {}).get("last_name", ""),
            "Column1.user_metadata.first_name": user.get("user_metadata", {}).get("first_name", ""),
            # 住所・社名（既存のネストパスを踏襲）
            "Column1.app_metadata.organization_data.metadata.company_address.postcode":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_address", {}).get("postcode", ""),
            "Column1.app_metadata.organization_data.metadata.company_address.prefecture":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_address", {}).get("prefecture", ""),
            "Column1.app_metadata.organization_data.metadata.company_address.city":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_address", {}).get("city", ""),
            "Column1.app_metadata.organization_data.metadata.company_name":
                user.get("app_metadata", {}).get("organization_data", {}).get("metadata", {}).get("company_name", "")
        }

        found = find_prm_expire(user)
        new_user["Column1.prm_expire_date_raw"] = found if found else ""
        new_user["Column1.prm_expire_date"] = _normalize_datetime(found) if found else ""
   
        new_users.append(new_user)
    return new_users

#--------------------------------------------------
# エクスポート処理（独立関数）
# これを app.py から呼び出すことで GUI から実行できます
#--------------------------------------------------
def export_all_users_to_files(domain, client_id, client_secret, audience, per_page=100):
    """
    実際のエクスポート処理を行い、出力ファイルのパスを返す。
    成功時: (True, {"json": json_path, "excel": excel_path})
    失敗時: (False, "エラーメッセージ")
    """
    try:
        print("アクセストークンを取得中...")
        access_token = get_access_token(domain, client_id, client_secret, audience)
        print("アクセストークン取得完了。")

        print("すべてのユーザー情報をセグメント別に取得中...")
        users = get_all_users_segmented(domain, access_token, per_page=per_page)
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

        return True, {"json": json_filename, "excel": excel_filename}
    except Exception as e:
        tb = traceback.format_exc()
        err_msg = f"{e}\n{tb}"
        print("エクスポート中にエラーが発生しました:", err_msg)
        return False, err_msg

#--------------------------------------------------
# run_export (GUI から呼ぶためのラッパ)
#--------------------------------------------------
def run_export():
    """
    app.py など GUI から呼び出す用の関数。
    成功時: (True, ファイルパス文字列) を返す
    失敗時: (False, エラーメッセージ) を返す
    """
    try:
        ok, info = export_all_users_to_files(domain, client_id, client_secret, audience, per_page=100)
        if ok:
            # info は辞書 {"json": ..., "excel": ...}
            return True, info.get("excel", info)
        else:
            return False, info
    except Exception as e:
        tb = traceback.format_exc()
        return False, f"{e}\n{tb}"

#--------------------------------------------------
# メイン処理（CLI 実行時）
#--------------------------------------------------
def main():
    ok, info = export_all_users_to_files(domain, client_id, client_secret, audience, per_page=100)
    # export_all_users_to_files が標準出力でメッセージを出しているため
    # ここでは追加処理は不要。ただし戻り値を使って終了コードを制御することは可能。
    if not ok:
        # エラー時は非ゼロコードで終了
        sys.exit(1)

if __name__ == "__main__":
    main()
