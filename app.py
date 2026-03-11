#アプリ化時のUI設定プログラム

# app.py
import threading
import tkinter as tk
from tkinter import messagebox
import time

# main.py の run_export を呼ぶ
import main

class ExportApp:
    def __init__(self, root):
        self.root = root
        root.title("Auth0 Exporter")
        root.geometry("320x110")
        root.resizable(False, False)

        self.label = tk.Label(root, text="起動しています…", font=("Yu Gothic UI", 11))
        self.label.pack(pady=12)

        self.status = tk.Label(root, text="アプリ実行中...", fg="blue")
        self.status.pack()

        # 自動で開始。ユーザ操作不要にするため Start ボタンは表示していません。
        self.thread = threading.Thread(target=self._run_task, daemon=True)
        self.thread.start()

        # メインループでスレッド終了を監視
        self._check_thread()

    def _run_task(self):
        # 少し待ってUIが表示されるのを安定させる（任意）
        time.sleep(0.3)
        try:
            ok, info = main.run_export()
            self.result_ok = ok
            self.result_info = info
        except Exception as e:
            self.result_ok = False
            self.result_info = f"例外: {e}"

    def _check_thread(self):
        if self.thread.is_alive():
            # 実行中
            self.status.config(text="アプリ実行中...")
            self.root.after(300, self._check_thread)
        else:
            # 終了
            if getattr(self, "result_ok", False):
                self.status.config(text="おわりました", fg="green")
                self.label.config(text="エクスポートが完了しました")
                messagebox.showinfo("完了", f"エクスポートが完了しました。\n{self.result_info}")
            else:
                self.status.config(text="エラー", fg="red")
                self.label.config(text="エクスポートに失敗しました")
                messagebox.showerror("エラー", f"エクスポート中にエラーが発生しました。\n{self.result_info}")
            # 終了後にウィンドウを自動で閉じたい場合は数秒後に root.destroy() する
            self.root.after(1000, self.root.destroy)

if __name__ == "__main__":
    root = tk.Tk()
    app = ExportApp(root)
    root.mainloop()
