import argparse
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
from PyQt6.QtGui import QKeySequence, QTextCursor
from PyQt6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

SPECIAL_KEYS_MAP = {
    Qt.Key.Key_Up: "\x1b[A",
    Qt.Key.Key_Down: "\x1b[B",
    Qt.Key.Key_Right: "\x1b[C",
    Qt.Key.Key_Left: "\x1b[D",
    Qt.Key.Key_Home: "\x1b[H",
    Qt.Key.Key_End: "\x1b[F",
    Qt.Key.Key_PageUp: "\x1b[5~",
    Qt.Key.Key_PageDown: "\x1b[6~",
    Qt.Key.Key_Insert: "\x1b[2~",
    Qt.Key.Key_Delete: "\x1b[3~",
    **{
        getattr(
            Qt.Key, f"Key_F{i}"
        ): f"\x1bO{chr(80+i-1)}" if i <= 4 else f"\x1b[{10+i+(1 if i>=6 else 0)+(1 if i>=10 else 0)}~"
        for i in range(1, 13)
    },
}


class CamWatchPane(QWidget):

    def __init__(self, stream_target="pcjrduino", parent=None):
        super().__init__(parent)
        self.stream_target = stream_target
        self.proc = None
        self.setStyleSheet("background-color: #000000; border: 1px solid #333;")

    def start_feed(self):
        if self.proc is not None:
            return
        wid = str(int(self.winId()))
        cmd = [
            "python3",
            "/home/k/scripts/camwatch.py",
            self.stream_target,
            "--",
            f"--wid={wid}",
            "--profile=low-latency",
            "--untimed",
        ]
        try:
            # start_new_session puts mpv and its spawned processes into their own process group
            self.proc = subprocess.Popen(cmd, start_new_session=True)
        except Exception as e:
            print(f"[ERROR] Failed to start camwatch.py: {e}", file=sys.stderr)

    def stop_feed(self):
        if self.proc and self.proc.poll() is None:
            try:
                # Forcefully kill the entire mpv process group on exit
                pgid = os.getpgid(self.proc.pid)
                os.killpg(pgid, signal.SIGKILL)
            except Exception:
                pass
            self.proc = None


class QueueWorker(threading.Thread):

    def __init__(
        self,
        char_queue,
        log_chunk_signal,
        status_signal,
        ssh_target="pcjrduino",
        cps=30.0,
    ):
        super().__init__(daemon=True)
        self.queue = char_queue
        self.log_chunk_signal = log_chunk_signal
        self.status_signal = status_signal
        self.ssh_target = ssh_target
        self.delay = 1.0 / cps if cps > 0 else 0.0
        self.abort_event = threading.Event()
        self.running = True
        self.line_buffer = bytearray()
        self.last_activity = time.time()
        self.ssh_proc = None

    def send_break_ssh(self):
        """Flushes queue and executes pycjr.py --cc via out-of-band SSH."""
        self.status_signal.emit("[OOB] Aborting queue and sending --cc...")
        self.abort()
        if self.ssh_target:
            cmd = ["ssh", self.ssh_target, "python3 /home/k/pcjr-cassio-lab/pycjr.py --cc"]
            try:
                subprocess.Popen(cmd, start_new_session=True)
            except Exception as e:
                self.status_signal.emit(f"[OOB ERROR] Failed to send --cc: {e}")

    def send_reset_ssh(self):
        """Flushes queue and executes pycjr.py --reset via out-of-band SSH."""
        self.status_signal.emit("[OOB] Aborting queue and sending --reset...")
        self.abort()
        if self.ssh_target:
            cmd = ["ssh", self.ssh_target, "python3 /home/k/pcjr-cassio-lab/pycjr.py --reset"]
            try:
                subprocess.Popen(cmd, start_new_session=True)
            except Exception as e:
                self.status_signal.emit(f"[OOB ERROR] Failed to send --reset: {e}")

    def start_ssh(self):
        if not self.ssh_target:
            self.status_signal.emit(
                "[SSH] No target specified. Running in dry-run mode."
            )
            return

        remote_cmd = "python3 -u /home/k/pcjr-cassio-lab/pycjr.py --cps -1 --stdin"
        cmd = ["ssh", "-t", "-t", self.ssh_target, remote_cmd]

        try:
            self.ssh_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=0,
                start_new_session=True,
            )

            self.status_signal.emit(
                f"[SSH] Connected binary PTY pipe to {self.ssh_target}"
            )
            time.sleep(1)

            # Handshake with pretty message
            if self.ssh_proc.stdin:
                self.ssh_proc.stdin.write(b"Connected\r")
                self.ssh_proc.stdin.flush()

            threading.Thread(
                target=self._read_remote_output, daemon=True
            ).start()

            time.sleep(1)

            self.status_signal.emit("[SSH] Please wait for handshake...")
        except Exception as e:
            self.status_signal.emit(
                f"[SSH ERROR] Failed to open SSH process: {e}"
            )

    def _read_remote_output(self):
        if not self.ssh_proc or not self.ssh_proc.stdout:
            return

        # Simple thread reader that terminates when the stream closes or process exits
        for line in iter(self.ssh_proc.stdout.readline, b""):
            if not self.running:
                break
            msg = line.decode("latin-1", errors="replace").rstrip()
            if msg:
                self.status_signal.emit(f"[REMOTE] {msg}")

    def _flush_buffer(self):
        if self.line_buffer:
            data = bytes(self.line_buffer)
            self.log_chunk_signal.emit(data)

            if self.ssh_proc and self.ssh_proc.stdin:
                try:
                    self.ssh_proc.stdin.write(data)
                    self.ssh_proc.stdin.flush()
                except Exception as e:
                    self.status_signal.emit(f"[SSH WRITE ERROR] {e}")

            self.line_buffer.clear()

    def run(self):
        self.start_ssh()
        while self.running:
            try:
                data = self.queue.get(timeout=0.05)
            except queue.Empty:
                continue

            if self.abort_event.is_set():
                self.queue.task_done()
                continue

            self.last_activity = time.time()
            encoded = (
                data.encode("latin-1", errors="replace")
                if isinstance(data, str)
                else bytes(data)
            )
            self.line_buffer.extend(encoded)

            self._flush_buffer()

            if self.delay > 0:
                time.sleep(self.delay)

            self.queue.task_done()

    def abort(self):
        self.abort_event.set()
        self.line_buffer.clear()
        drained = 0
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
                drained += 1
            except queue.Empty:
                break
        self.status_signal.emit(
            f"[ABORT] Queue flushed! Discarded {drained} pending items."
        )
        self.abort_event.clear()

    def stop(self):
        self.running = False
        if self.ssh_proc:
            try:
                pgid = os.getpgid(self.ssh_proc.pid)
                os.killpg(pgid, signal.SIGKILL)
            except Exception:
                pass


class InputTerminal(QPlainTextEdit):

    PROMPT = "> "

    def __init__(self, char_queue, parent=None):
        super().__init__(parent)
        self.queue = char_queue
        self.ready = False  # Disabled on launch
        self.setStyleSheet(
            "background-color: #0c0c0c; color: #00ff66; font-family: monospace; font-size: 13px;"
        )
        self.insertPlainText("[CONNECTING...]\n")

    def set_ready(self):
        self.ready = True
        self.clear()
        self.append_prompt()

    def set_handshake(self):
        self.setPlainText("[WAITING FOR HANDSHAKE...]\n")
        QTimer.singleShot(3000, self.set_ready)

    def append_prompt(self):
        cursor = self.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.setTextCursor(cursor)
        self.insertPlainText(self.PROMPT)
        self.scroll_to_bottom()

    def scroll_to_bottom(self):
        self.verticalScrollBar().setValue(self.verticalScrollBar().maximum())

    def _current_line_prompt_pos(self):
        block = self.document().lastBlock()
        return (
            block.position() + len(self.PROMPT)
            if block.text().startswith(self.PROMPT)
            else block.position()
        )

    def keyPressEvent(self, event):
        if not self.ready:
            return
        min_pos = self._current_line_prompt_pos()
        if self.textCursor().position() < min_pos:
            cursor = self.textCursor()
            cursor.setPosition(min_pos)
            self.setTextCursor(cursor)

        if event.matches(QKeySequence.StandardKey.Paste) or (
            event.modifiers() & Qt.KeyboardModifier.ControlModifier
            and event.key() == Qt.Key.Key_V
        ):
            text = (
                QApplication.clipboard().text().replace("\r\n", "\r").replace("\n", "\r")
            )
            for char in text:
                self.insertPlainText("\n" + self.PROMPT if char == "\r" else char)
                self.queue.put(char)
            self.scroll_to_bottom()
            return

        key = event.key()
        if key in SPECIAL_KEYS_MAP:
            self.queue.put(SPECIAL_KEYS_MAP[key])
            return

        if (
            event.modifiers() & Qt.KeyboardModifier.ControlModifier
            and key in (Qt.Key.Key_Pause, Qt.Key.Key_Cancel)
        ):
            self.queue.put("\x1b[30~")
            return

        if key in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            self.queue.put("\r")
            self.insertPlainText("\n" + self.PROMPT)
        elif key == Qt.Key.Key_Backspace and self.textCursor().position() > min_pos:
            self.queue.put("\b")
            self.textCursor().deletePreviousChar()
        elif key == Qt.Key.Key_Tab:
            self.queue.put("\t")
            self.insertPlainText("    ")
        elif key == Qt.Key.Key_Escape:
            self.queue.put("\x1b")
        elif event.text() and not (
            event.modifiers() & Qt.KeyboardModifier.ControlModifier
        ):
            self.queue.put(event.text())
            self.insertPlainText(event.text())
        else:
            super().keyPressEvent(event)

        self.scroll_to_bottom()


class ControlPanelApp(QWidget):

    log_chunk_signal = pyqtSignal(bytes)
    status_signal = pyqtSignal(str)

    def __init__(self, stream_target="pcjrduino", ssh_target=None, cps=30.0):
        super().__init__()
        self.cps = cps
        self.setWindowTitle("PyQt6 PCjr Control Panel Bridge")
        self.resize(1100, 550)

        self.raw_history = []
        self.char_queue = queue.Queue()

        self.log_chunk_signal.connect(lambda b: self._append_entry(("data", b)))
        self.status_signal.connect(lambda s: self._append_entry(("status", s)))

        self.worker = QueueWorker(
            self.char_queue,
            self.log_chunk_signal,
            self.status_signal,
            ssh_target=ssh_target,
            cps=self.cps,
        )
        self.worker.start()

        self.stream_target = stream_target
        self.init_ui()

        QTimer.singleShot(3000, self.terminal.set_handshake)

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        top = QHBoxLayout()
        top.addWidget(QLabel(f"Status: Pacing {self.cps} CPS"))
        top.addStretch()
        abort_btn = QPushButton("ABORT PASTE / CLEAR QUEUE")
        abort_btn.setStyleSheet(
            "background-color: #d9534f; color: white; font-weight: bold; padding: 6px;"
        )
        abort_btn.clicked.connect(self.worker.abort)
        top.addWidget(abort_btn)
        main_layout.addLayout(top)

        # Create control action buttons
        break_btn = QPushButton("CTRL+BREAK")
        break_btn.setStyleSheet(
            "background-color: #f0ad4e; color: white; font-weight: bold; padding: 6px;"
        )
        break_btn.clicked.connect(
            lambda: self.worker.send_break_ssh() if hasattr(self, "worker") else None
        )

        reset_btn = QPushButton("RESET (CTRL+ALT+DEL)")
        reset_btn.setStyleSheet(
            "background-color: #d9534f; color: white; font-weight: bold; padding: 6px;"
        )
        reset_btn.clicked.connect(
            lambda: self.worker.send_reset_ssh() if hasattr(self, "worker") else None
        )

        # Place them into your layout before abort_btn (or swap in your layout variable name)
        # Assuming header layout or main_layout:
        top.addWidget(break_btn)
        top.addWidget(reset_btn)
        top.addWidget(abort_btn)

        video_box = QVBoxLayout()
        video_box.addWidget(QLabel(f"CRT Feed ({self.stream_target}):"))
        self.video_pane = CamWatchPane(stream_target=self.stream_target, parent=self)
        video_box.addWidget(self.video_pane, stretch=2)
        main_layout.addLayout(video_box, stretch=2)

        term_box = QVBoxLayout()
        term_box.addWidget(QLabel("Raw Key Capture Terminal:"))
        self.terminal = InputTerminal(self.char_queue, parent=self)
        term_box.addWidget(self.terminal, stretch=1)
        main_layout.addLayout(term_box, stretch=1)

        drawer_bar = QHBoxLayout()
        self.toggle_btn = QPushButton("Show Transmission Log Drawer ▲")
        self.toggle_btn.setCheckable(True)
        self.toggle_btn.clicked.connect(
            lambda c: (
                self.log_view.setVisible(c),
                self.toggle_btn.setText(
                    f"{'Hide' if c else 'Show'} Transmission Log Drawer {'▼' if c else '▲'}"
                ),
            )
        )

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Line Stream", "Hex Dump", "Verbose Trace"])
        self.mode_combo.currentIndexChanged.connect(self.re_render_logs)

        drawer_bar.addWidget(self.toggle_btn)
        drawer_bar.addStretch()
        drawer_bar.addWidget(QLabel("Log View Mode:"))
        drawer_bar.addWidget(self.mode_combo)
        main_layout.addLayout(drawer_bar)

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumHeight(60)
        self.log_view.setStyleSheet(
            "background-color: #111; color: #888; font-family: monospace; font-size: 11px;"
        )

        self.log_view.setVisible(True)
        main_layout.addWidget(self.log_view)

    def showEvent(self, event):
        super().showEvent(event)
        self.video_pane.start_feed()
        self.terminal.setFocus()

    def closeEvent(self, event):
        self.video_pane.stop_feed()
        self.worker.stop()
        super().closeEvent(event)

    def _append_entry(self, item):
        self.raw_history.append(item)
        formatted = self.format_entry(item, self.mode_combo.currentText())
        if formatted:
            self.log_view.appendPlainText(formatted)
            self.log_view.verticalScrollBar().setValue(
                self.log_view.verticalScrollBar().maximum()
            )

    def format_entry(self, item: tuple, mode: str) -> str:
        kind, content = item
        if kind == "status":
            return f"[SYS] {content}"
        if not content:
            return ""

        if mode == "Line Stream":
            return f"[TX Line] {repr(content.decode('latin-1', errors='replace'))} ({content.hex()})"
        if mode == "Hex Dump":
            return f"[TX Hex] {' '.join(f'{b:02x}' for b in content)}"
        if mode == "Verbose Trace":
            return "\n".join(
                f"[TX Byte] {(repr(chr(b)) if 32 <= b <= 126 else f'\\x{b:02x}'):<6} ({b:02x})"
                for b in content
            )
        return ""

    def re_render_logs(self):
        self.log_view.clear()
        mode = self.mode_combo.currentText()
        lines = [
            formatted
            for item in self.raw_history
            if (formatted := self.format_entry(item, mode))
        ]
        if lines:
            self.log_view.setPlainText("\n".join(lines))
            self.log_view.verticalScrollBar().setValue(
                self.log_view.verticalScrollBar().maximum()
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PCjr Control Panel Bridge")
    parser.add_argument(
        "--stream", default="pcjrduino", help="Stream target alias for camwatch.py"
    )
    parser.add_argument(
        "--ssh", default="pcjrduino", help="SSH target host (e.g., k@pi-ip or alias)"
    )
    parser.add_argument(
        "--cps", type=int, default=30.0, help="Set delay between successive frames, default is 30 chars/sec"
    )
    args = parser.parse_args()

    app = QApplication(sys.argv)
    window = ControlPanelApp(stream_target=args.stream, ssh_target=args.ssh, cps=args.cps)
    window.show()
    sys.exit(app.exec())
