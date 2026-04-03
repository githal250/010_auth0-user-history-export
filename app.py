import threading, tkinter as tk
from tkinter import messagebox
import queue, main

class ExportApp:
    def __init__(self, root):
        self.root = root
        self.q = queue.Queue()
        root.title("Auth0 Exporter")
        root.geometry("360x130")
        root.resizable(False, False)
        self.label = tk.Label(root, text="起動しています…", font=("Yu Gothic UI", 11))
        self.label.pack(pady=10)
        self.status = tk.Label(root, text="待機中", fg="blue")
        self.status.pack()
        self.thread = threading.Thread(target=self._run_task, daemon=True)
        self.thread.start()
        self._poll()

    def _progress(self, msg):
        self.q.put(msg)

    def _run_task(self):
        try:
            ok, info = main.run_export(progress_callback=self._progress)
            self.result_ok, self.result_info = ok, info
        except Exception as e:
            self.result_ok, self.result_info = False, f"例外: {e}"

    def _poll(self):
        while not self.q.empty():
            msg = self.q.get_nowait()
            self.status.config(text=msg)
        if self.thread.is_alive():
            self.root.after(200, self._poll)
        else:
            if getattr(self, "result_ok", False):
                self.label.config(text="エクスポートが完了しました")
                self.status.config(text="完了", fg="green")
                messagebox.showinfo("完了", str(self.result_info))
            else:
                self.label.config(text="エクスポートに失敗しました")
                self.status.config(text="エラー", fg="red")
                messagebox.showerror("エラー", str(self.result_info))
            self.root.after(1000, self.root.destroy)

if __name__ == "__main__":
    root = tk.Tk()
    ExportApp(root)
    root.mainloop()