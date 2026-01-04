from __future__ import annotations
import threading
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from ..settings import Settings, ensure_app_dirs
from .. import db as dbm
from ..scanner import scan, ScanResult
from ..duplicate import get_duplicates
from ..organizer import parse_filename, propose_path


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, settings: Settings):
        super().__init__()
        self.settings = settings
        ensure_app_dirs(self.settings)
        self.conn = dbm.connect(self.settings.db_path)
        self.setWindowTitle("Media Library Manager")
        self.resize(1000, 700)

        # Tabs as central widget
        self.tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self.tabs)

        # Status bar for global status (server/agent)
        self.status = QtWidgets.QStatusBar()
        self.setStatusBar(self.status)
        self._status_server = QtWidgets.QLabel("Server: unknown")
        self._status_agent = QtWidgets.QLabel("Agent: idle")
        self.status.addPermanentWidget(self._status_server)
        self.status.addPermanentWidget(self._status_agent)
        self._start_status_updates()
        # Auto-start ingestion server for convenience
        try:
            from ..ingest_server import start_server
            start_server(self.settings)
        except Exception:
            pass
        self._init_library_tab()
        self._init_duplicates_tab()
        self._init_organizer_tab()
        self._init_junk_tab()
        self._init_settings_tab()
        self._refresh_roots()
        # Load persisted UI settings (agent IP and configurable fields)
        try:
            self._load_prefs()
        except Exception:
            pass

    # Scan Tab
    def _init_scan_tab(self):
        w = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(w)
        note = QtWidgets.QLabel("Scanning runs in the background. Status and agent connection shown in the status bar below.")
        note.setWordWrap(True)
        layout.addWidget(note)
        self.tabs.addTab(w, "Scan")

    def _browse_root(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Root Folder")
        if d:
            self.path_edit.setText(d)

    def _start_scan(self):
        from ..db import enabled_roots
        manual_root = self.path_edit.text().strip()
        if manual_root:
            roots = [Path(manual_root)]
        else:
            roots = enabled_roots(self.conn)
            if not roots:
                QtWidgets.QMessageBox.warning(self, "No roots", "Add or select a root in Settings or specify a folder.")
                return
        self.progress_bar.setVisible(True)
        self.btn_scan.setEnabled(False)
        self.progress_label.setText("Scanning...")

        def progress_cb(res: ScanResult):
            self.progress_label.setText(f"Processed: {res.files_processed} | Metadata: {res.metadata_count} | Hashed: {res.hashed_count}")
            # Throttle-Refresh duplicates view every 50 files
            if res.files_processed % 50 == 0:
                QtCore.QMetaObject.invokeMethod(self, "_refresh_duplicates", QtCore.Qt.QueuedConnection)

        def worker():
            scan(self.conn, roots, self.settings, progress_cb)
            QtCore.QMetaObject.invokeMethod(self, "_scan_done", QtCore.Qt.QueuedConnection)

        threading.Thread(target=worker, daemon=True).start()

    @QtCore.Slot()
    def _scan_done(self):
        self._refresh_library()
        self._refresh_duplicates()

    def _start_local_roots_scan(self):
        from ..db import enabled_roots
        roots = enabled_roots(self.conn)
        if not roots:
            QtWidgets.QMessageBox.information(self, "Scan", "No enabled local roots. Add/enable in Settings > Local Roots.")
            return
        self._status_agent.setText("Local scan: starting...")
        try:
            self.pb_local.setVisible(True)
        except Exception:
            pass
        def progress_cb(res: ScanResult):
            self._status_agent.setText(f"Local scan: {res.files_processed} processed, {res.metadata_count} meta, {res.hashed_count} hashed")
            if res.files_processed % 100 == 0:
                QtCore.QMetaObject.invokeMethod(self, "_refresh_library", QtCore.Qt.QueuedConnection)
        # Kick off background count to enable determinate progress
        def count_worker():
            try:
                # late import to avoid circulars
                from ..scanner import iter_media_files
                total = 0
                for _ in iter_media_files(roots, self.settings):
                    total += 1
                # switch to determinate when known
                def _apply():
                    try:
                        self.pb_local.setRange(0, max(1, total))
                        self.pb_local.setValue(0)
                        self.pb_local.setFormat(f"Local scan: 0/{total}")
                    except Exception:
                        pass
                QtCore.QMetaObject.invokeMethod(self, "_apply_progress_local", QtCore.Qt.QueuedConnection) if False else None
                # apply directly via lambda
                QtCore.QTimer.singleShot(0, _apply)
                self._local_total = total
            except Exception:
                pass
        threading.Thread(target=count_worker, daemon=True).start()
        # Start scan
        def worker():
            try:
                self._local_total = getattr(self, '_local_total', 0)
                def _cb(res: ScanResult):
                    if getattr(self, '_local_total', 0) > 0:
                        try:
                            self.pb_local.setMaximum(max(1, self._local_total))
                            self.pb_local.setValue(res.files_processed)
                            self.pb_local.setFormat(f"Local scan: {res.files_processed}/{self._local_total}")
                        except Exception:
                            pass
                    progress_cb(res)
                scan(self.conn, roots, self.settings, _cb)
            except Exception as e:
                QtCore.QMetaObject.invokeMethod(self, "_set_status_info", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Scan error: {e}"))
            finally:
                QtCore.QMetaObject.invokeMethod(self, "_scan_done", QtCore.Qt.QueuedConnection)
        threading.Thread(target=worker, daemon=True).start()

    def _show_agent_cache_info(self):
        ipw = getattr(self, 'edit_agent_ip', None)
        ip = ipw.text().strip() if ipw else ""
        if not ip:
            QtWidgets.QMessageBox.information(self, "Agent Cache", "Enter the Agent IP in Settings > Remote & Server.")
            return
        try:
            import requests
            r = requests.get(f"http://{ip}:8877/agent/cache_info", timeout=2)
            if r.ok:
                info = r.json()
                txt = (
                    f"Path: {info.get('db_path','')}\n"
                    f"Exists: {info.get('exists')}\n"
                    f"Size: {info.get('size_bytes',0)} B ({info.get('size_mib',0)} MiB)\n"
                    f"Rows: index={info.get('rows',{}).get('agent_index',0)}, outbox={info.get('rows',{}).get('outbox',0)}, progress={info.get('rows',{}).get('scan_progress',0)}\n"
                )
                QtWidgets.QMessageBox.information(self, "Agent Cache Info", txt)
            else:
                QtWidgets.QMessageBox.warning(self, "Agent Cache Info", f"Agent returned {r.status_code}")
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Agent Cache Info", f"Failed: {e}")

    def _clear_agent_cache(self):
        ipw = getattr(self, 'edit_agent_ip', None)
        ip = ipw.text().strip() if ipw else ""
        if not ip:
            QtWidgets.QMessageBox.information(self, "Clear Agent Cache", "Enter the Agent IP in Settings > Remote & Server.")
            return
        if QtWidgets.QMessageBox.question(self, "Clear Agent Cache", "This will delete the agent's local cache file. Continue?") != QtWidgets.QMessageBox.Yes:
            return
        try:
            import requests
            r = requests.post(f"http://{ip}:8877/agent/clear_cache", timeout=3)
            if r.ok and r.json().get('ok'):
                QtWidgets.QMessageBox.information(self, "Clear Agent Cache", "Agent cache cleared.")
            else:
                QtWidgets.QMessageBox.warning(self, "Clear Agent Cache", f"Agent returned {r.status_code}")
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Clear Agent Cache", f"Failed: {e}")

    def _start_remote_scan(self):
        ipw = getattr(self, 'edit_agent_ip', None)
        ip = ipw.text().strip() if ipw else ""
        if not ip:
            QtWidgets.QMessageBox.information(self, "Remote Scan", "Enter the Agent IP in Settings > Remote & Server.")
            return
        try:
            import requests, time as _t
            r = requests.post(f"http://{ip}:8877/agent/scan_now", timeout=2)
            if r.ok and (r.json().get('ok') or r.json().get('started')):
                self._status_agent.setText("Agent: remote scan started")
                try:
                    self.pb_remote.setVisible(True)
                except Exception:
                    pass
            else:
                QtWidgets.QMessageBox.warning(self, "Remote Scan", f"Agent did not accept scan: {r.status_code}")
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Remote Scan", f"Failed to contact agent: {e}")

    # Status bar updates
    def _start_status_updates(self):
        self._status_timer = QtCore.QTimer(self)
        self._status_timer.setInterval(2000)
        self._status_timer.timeout.connect(self._update_status_bar)
        self._status_timer.start()
        self._update_status_bar()

    def _update_status_bar(self):
        try:
            from ..ingest_server import get_ingest_stats
            st = get_ingest_stats()
            srv = "running" if st.get("running") else "stopped"
            last = st.get("last_ingest_ts")
            cnt = st.get("last_ingest_count", 0)
            if last:
                import time as _t
                ago = int(_t.time() - (last or 0))
                self._status_server.setText(f"Server: {srv}, last ingest {ago}s ago ({cnt} files)")
            else:
                self._status_server.setText(f"Server: {srv}")
        except Exception:
            self._status_server.setText("Server: n/a")
        # Agent status with optional live stats from agent if IP is set
        try:
            ip = getattr(self, 'edit_agent_ip', None)
            ip_txt = ip.text().strip() if ip else ""
            if ip_txt:
                import requests
                r = requests.get(f"http://{ip_txt}:8877/agent/stats", timeout=1.5)
                if r.ok:
                    s = r.json()
                    if s.get('elapsed') is not None or s.get('active') is True:
                        active = s.get('active') is True
                        prefix = "Agent (scanning)" if active else "Agent"
                        total = int(s.get('total_all') or 0)
                        seen = int(s.get('seen') or 0)
                        if total > 0:
                            try:
                                self.pb_remote.setRange(0, max(1, total))
                                self.pb_remote.setValue(min(seen, total))
                                b = int(s.get('batches') or 0)
                                rate = float(s.get('rate_files_per_s') or 0.0)
                                phase = s.get('phase_name') or ''
                                phase_str = f" ({phase})" if phase else ''
                                self.pb_remote.setFormat(f"Remote scan: {seen}/{total}{phase_str} | batches: {b} | rate: {rate:.1f}/s")
                            except Exception:
                                pass
                        self._status_agent.setText(f"{prefix}: {s.get('uploaded',0)} up, {s.get('rate_files_per_s',0):.1f}/s, {s.get('data_mib',0):.1f} MiB in {s.get('elapsed',0):.1f}s")
                        try:
                            self.pb_remote.setVisible(active)
                        except Exception:
                            pass
                        return
            self._status_agent.setText("Agent: idle")
        except Exception:
            self._status_agent.setText("Agent: idle")

    # Helpers to update per-tab status summaries and refresh on tab switch
    def _human_size(self, n):
        try:
            n = int(n or 0)
        except Exception:
            return "0 B"
        units = ['B','KB','MB','GB','TB','PB']
        i = 0
        f = float(n)
        while f >= 1024 and i < len(units)-1:
            f /= 1024.0
            i += 1
        return f"{f:.1f} {units[i]}" if n else "0 B"

    def _on_tab_changed(self, idx: int):
        try:
            label = self.tabs.tabText(idx)
        except Exception:
            label = ""
        if label == "Library":
            self._refresh_library()
        elif label == "Duplicates":
            self._refresh_duplicates()
        elif label == "Junk":
            # Junk tab is a separate widget; we only update the status summary here
            self._update_junk_status()

    def _update_library_status(self):
        try:
            from ..db import fetch_library_rows
            rows = fetch_library_rows(self.conn)
            count = len(rows)
            total = sum(int(r[1] or 0) for r in rows)  # r[1] = size
            self._set_status_info(f"Library: {count} files | Total size: {self._human_size(total)}")
        except Exception:
            pass

    def _update_duplicates_status(self, rows=None):
        if rows is None:
            try:
                from ..db import fetch_duplicate_rows
                rows = fetch_duplicate_rows(self.conn, include_suspected=True)
            except Exception:
                rows = []
        from collections import defaultdict
        groups = defaultdict(list)
        for r in rows:
            groups[r[0]].append(r)
        total_wasted = 0
        dup_files = 0
        for g, items in groups.items():
            if len(items) > 1:
                dup_files += len(items)
                size = int(items[0][2] or 0)  # r[2] = size
                total_wasted += size * (len(items)-1)
        self._set_status_info(f"Duplicates: {dup_files} files | Wasted: {self._human_size(total_wasted)}")

    def _update_junk_status(self):
        try:
            from ..db import list_junk
            rows = list_junk(self.conn)
            count = len(rows)
            total = sum(int(r[1] or 0) for r in rows)
            self._set_status_info(f"Junk: {count} files | Total size: {self._human_size(total)}")
        except Exception:
            pass

    def _set_status_info(self, text: str):
        try:
            if not hasattr(self, '_status_info'):
                self._status_info = QtWidgets.QLabel()
                self.status.insertWidget(0, self._status_info, 1)
            self._status_info.setText(text)
        except Exception:
            pass

    # Library Tab
    def _init_library_tab(self):
        w = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(w)

        toolbar = QtWidgets.QHBoxLayout()
        self.search_edit = QtWidgets.QLineEdit()
        self.search_edit.setPlaceholderText("Filter by path or codec...")
        self.combo_cat = QtWidgets.QComboBox()
        self.combo_cat.addItems(["All", "video", "image", "subtitle", "xml", "other", "unknown"])
        btn_reload = QtWidgets.QPushButton("Reload")
        btn_reload.clicked.connect(self._refresh_library)
        toolbar.addWidget(self.search_edit)
        toolbar.addWidget(QtWidgets.QLabel("Category:"))
        toolbar.addWidget(self.combo_cat)
        toolbar.addWidget(btn_reload)
        layout.addLayout(toolbar)

        self.table = QtWidgets.QTableView()
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)

        self.tabs.addTab(w, "Library")
        self._setup_library_model()
        self._refresh_library()
        # React to tab changes to refresh current tab and update status
        self.tabs.currentChanged.connect(self._on_tab_changed)

    def _setup_library_model(self):
        from PySide6.QtGui import QStandardItemModel, QStandardItem
        self.lib_model = QStandardItemModel()
        headers = [
            "Name", "Path", "Size", "Modified", "Ext", "Category", "Show", "Season", "Episode", "Title", "Duration", "Container", "Video", "Audio", "Width", "Height", "Bitrate", "Sample Hash", "Full Hash",
        ]
        self.lib_model.setHorizontalHeaderLabels(headers)
        from PySide6.QtCore import QSortFilterProxyModel, Qt
        class CategoryFilterProxy(QSortFilterProxyModel):
            def __init__(self, parent=None):
                super().__init__(parent)
                self._category = "All"
                self._search = ""
            def setCategory(self, cat: str):
                self._category = cat or "All"
                self.invalidateFilter()
            def setSearch(self, text: str):
                self._search = text or ""
                self.invalidateFilter()
            def filterAcceptsRow(self, source_row, source_parent):
                model = self.sourceModel()
                # columns: Name(0), Path(1), Size(2), Modified(3), Ext(4), Category(5), Show(6)...
                idx_path = model.index(source_row, 1, source_parent)
                idx_cat = model.index(source_row, 5, source_parent)
                path = (model.data(idx_path) or "").lower()
                cat = (model.data(idx_cat) or "")
                if self._search and self._search.lower() not in path:
                    return False
                if self._category != "All" and cat != self._category:
                    return False
                return True
        self.lib_proxy = CategoryFilterProxy(self)
        self.lib_proxy.setSourceModel(self.lib_model)
        self.table.setModel(self.lib_proxy)
        self.search_edit.textChanged.connect(self.lib_proxy.setSearch)
        self.combo_cat.currentTextChanged.connect(self.lib_proxy.setCategory)
        header = self.table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setMinimumSectionSize(40)
        for i in range(self.lib_model.columnCount()):
            header.setSectionResizeMode(i, QtWidgets.QHeaderView.Interactive)
        header.setSectionsMovable(True)
        # Set sensible initial widths
        # Name, Path, Size, Modified
        if self.lib_model.columnCount() > 0:
            self.table.setColumnWidth(0, 220)
        if self.lib_model.columnCount() > 1:
            self.table.setColumnWidth(1, 420)
        if self.lib_model.columnCount() > 2:
            self.table.setColumnWidth(2, 90)
        if self.lib_model.columnCount() > 3:
            self.table.setColumnWidth(3, 150)

    def _refresh_library(self):
        from PySide6.QtGui import QStandardItem
        from ..db import fetch_library_rows
        self.lib_model.removeRows(0, self.lib_model.rowCount())
        rows = fetch_library_rows(self.conn)
        # (removed unused pre-loop unpack to avoid mismatched column count after adding 'category')
        # Build rows
        from PySide6.QtGui import QStandardItem
        import time
        from ..organizer import parse_filename, parse_from_path
        def _human_size(n):
            try:
                n = int(n or 0)
            except Exception:
                return ""
            units = ['B','KB','MB','GB','TB','PB']
            i = 0
            f = float(n)
            while f >= 1024 and i < len(units)-1:
                f /= 1024.0
                i += 1
            return f"{f:.1f} {units[i]}" if n else ""
        def _human_time(ts):
            try:
                return time.strftime('%Y-%m-%d %H:%M', time.localtime(int(ts))) if ts else ""
            except Exception:
                return ""
        for row in rows:
            path, size, mtime, ext, category, duration, container, vcodec, acodecs, width, height, bitrate, shash, fhash = row
            p = Path(path)
            parsed = parse_filename(p.name) or parse_from_path(p)
            show = parsed.show if parsed else ""
            season = f"{parsed.season:02d}" if parsed else ""
            episode = f"{parsed.episode:02d}" if parsed else ""
            title = parsed.title or "" if parsed else ""
            values = [
                p.name, str(path), _human_size(size), _human_time(mtime), str(ext or ""), str(category or ""),
                show, season, episode, title,
                (f"{duration:.2f}" if isinstance(duration, (int, float)) else ""),
                str(container or ""), str(vcodec or ""), str(acodecs or ""),
                str(width or ""), str(height or ""), str(bitrate or ""), str(shash or ""), str(fhash or ""),
            ]
            items = [QStandardItem(v) for v in values]
            self.lib_model.appendRow(items)
        # Update status bar info for library
        self._update_library_status()

    # Duplicates Tab
    def _init_duplicates_tab(self):
        w = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(w)

        toolbar = QtWidgets.QHBoxLayout()
        self.dup_search = QtWidgets.QLineEdit()
        self.dup_search.setPlaceholderText("Filter duplicates by path or codec...")
        btn_reload = QtWidgets.QPushButton("Reload")
        btn_reload.clicked.connect(self._refresh_duplicates)
        self.chk_dup_suspect = QtWidgets.QCheckBox("Include suspected (size + sample hash)")
        self.chk_dup_suspect.setChecked(True)
        self.chk_dup_suspect.stateChanged.connect(lambda _: self._refresh_duplicates())
        toolbar.addWidget(self.dup_search)
        toolbar.addWidget(self.chk_dup_suspect)
        toolbar.addWidget(QtWidgets.QLabel("Rows are color-grouped by duplicate set; 'Reason' indicates why they're grouped."))
        toolbar.addWidget(btn_reload)
        layout.addLayout(toolbar)

        self.dup_table = QtWidgets.QTableView()
        self.dup_table.setSortingEnabled(True)
        self.dup_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.dup_table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        layout.addWidget(self.dup_table)

        # Actions for duplicates
        actions = QtWidgets.QHBoxLayout()
        self.btn_keep_trash_others = QtWidgets.QPushButton("Keep Selected, Trash Others in Group")
        self.btn_keep_trash_others.clicked.connect(self._act_keep_trash_others)
        self.btn_trash_selected = QtWidgets.QPushButton("Move Selected to Trash")
        self.btn_trash_selected.clicked.connect(self._act_trash_selected)
        self.btn_delete_selected = QtWidgets.QPushButton("Delete Selected (Permanent)")
        self.btn_delete_selected.clicked.connect(self._act_delete_selected)
        actions.addWidget(self.btn_keep_trash_others)
        actions.addWidget(self.btn_trash_selected)
        actions.addWidget(self.btn_delete_selected)
        layout.addLayout(actions)

        self.tabs.addTab(w, "Duplicates")
        self._setup_duplicates_model()
        # Hide group key column by default
        try:
            self.dup_table.setColumnHidden(0, True)
        except Exception:
            pass
        
        # Footer stats button and calculator
        self.dup_status = QtWidgets.QPushButton("Duplicates: 0 | Wasted: 0 B")
        self.dup_status.setFlat(True)
        self.dup_status.setToolTip("Click to refresh duplicates")
        self.dup_status.clicked.connect(self._refresh_duplicates)
        layout.addWidget(self.dup_status)
        
        def _update_dup_stats(rows):
            from collections import defaultdict
            groups = defaultdict(list)
            for r in rows:
                groups[r[0]].append(r)
            total_wasted = 0
            dup_files = 0
            for g, items in groups.items():
                if len(items) > 1:
                    dup_files += len(items)
                    size = int(items[0][2] or 0)
                    total_wasted += size * (len(items)-1)
            self.dup_status.setText(f"Duplicates: {dup_files} | Wasted: {self._human_size(total_wasted)}")
        self._update_dup_stats = _update_dup_stats

    def _setup_duplicates_model(self):
        from PySide6.QtGui import QStandardItemModel
        from PySide6.QtCore import QSortFilterProxyModel, Qt
        self.dup_model = QStandardItemModel()
        headers = [
            "Group", "Name", "Path", "Size", "Modified", "Ext", "Reason", "Duration", "Container", "Video", "Audio", "Width", "Height", "Bitrate", "Sample Hash", "Full Hash",
        ]
        self.dup_model.setHorizontalHeaderLabels(headers)
        self.dup_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.dup_table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.dup_proxy = QSortFilterProxyModel(self)
        self.dup_proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.dup_proxy.setFilterKeyColumn(2)  # Path column (index 2), since 0=Group,1=Name
        self.dup_proxy.setSourceModel(self.dup_model)
        self.dup_table.setModel(self.dup_proxy)
        self.dup_search.textChanged.connect(self.dup_proxy.setFilterFixedString)
        dup_header = self.dup_table.horizontalHeader()
        dup_header.setStretchLastSection(False)
        dup_header.setMinimumSectionSize(40)
        for i in range(self.dup_model.columnCount()):
            dup_header.setSectionResizeMode(i, QtWidgets.QHeaderView.Interactive)
        # Narrow the Select column
        if self.dup_model.columnCount() > 0:
            self.dup_table.setColumnWidth(0, 40)
        if self.dup_model.columnCount() > 2:
            self.dup_table.setColumnWidth(2, 500)  # Path

    def _color_for_group(self, key: str):
        # Deterministic pastel color from hash prefix
        import hashlib
        h = hashlib.sha1((key or "").encode("utf-8")).digest()
        r, g, b = h[0], h[1], h[2]
        # pastelize
        r = (r + 255) // 2
        g = (g + 255) // 2
        b = (b + 255) // 2
        from PySide6.QtGui import QColor
        return QColor(r, g, b, 80)

    def _refresh_duplicates(self):
        from PySide6.QtGui import QStandardItem
        from ..db import fetch_duplicate_rows
        self.dup_model.removeRows(0, self.dup_model.rowCount())
        rows = fetch_duplicate_rows(self.conn, include_suspected=self.chk_dup_suspect.isChecked())
        # Update footer stats
        self._update_dup_stats(rows)
        # Update status bar info for duplicates
        self._update_duplicates_status(rows)
        from PySide6.QtCore import Qt
        import time
        def _human_size(n):
            try:
                n = int(n or 0)
            except Exception:
                return ""
            units = ['B','KB','MB','GB','TB','PB']
            i = 0
            f = float(n)
            while f >= 1024 and i < len(units)-1:
                f /= 1024.0
                i += 1
            return f"{f:.1f} {units[i]}" if n else ""
        def _human_time(ts):
            try:
                return time.strftime('%Y-%m-%d %H:%M', time.localtime(int(ts))) if ts else ""
            except Exception:
                return ""
        from pathlib import Path as _P
        for row in rows:
            group_key, path, size, mtime, ext, duration, container, vcodec, acodecs, width, height, bitrate, shash, fhash = row
            # Reason for duplicate grouping
            reason = "full hash" if fhash else "sample hash + size"
            name = _P(path).name
            values = [
                str(group_key or ""), name, str(path), _human_size(size), _human_time(mtime), str(ext or ""), reason,
                (f"{duration:.2f}" if isinstance(duration, (int, float)) else ""),
                str(container or ""), str(vcodec or ""), str(acodecs or ""),
                str(width or ""), str(height or ""), str(bitrate or ""), str(shash or ""), str(fhash or ""),
            ]
            items = [QStandardItem(v) for v in values]
            # Color rows by group
            bg = self._color_for_group(group_key or "")
            for it in items:
                it.setBackground(bg)
            self.dup_model.appendRow(items)

    def _dup_selected_rows(self):
        # returns list of dicts with keys: group, path using checked boxes if any, else selected rows
        result = []
        # collect checked
        for r in range(self.dup_model.rowCount()):
            it = self.dup_model.item(r, 0)
            if it and it.isCheckable() and it.checkState() == QtCore.Qt.Checked:
                group_key = self.dup_model.item(r, 1).text()
                path = self.dup_model.item(r, 2).text()
                result.append({"group": group_key, "path": path})
        if result:
            return result
        # fallback to selection
        sel = self.dup_table.selectionModel().selectedRows()
        for idx in sel:
            src = self.dup_proxy.mapToSource(idx)
            group_key = self.dup_model.item(src.row(), 1).text()
            path = self.dup_model.item(src.row(), 2).text()
            result.append({"group": group_key, "path": path})
        return result

    def _perform_delete(self, paths: list[str], permanent: bool = False):
        import os, json
        from send2trash import send2trash
        from ..db import log_operation, delete_file_entry
        errors = []
        for p in paths:
            ok = True
            try:
                if permanent or not self.settings.use_trash:
                    os.remove(p)
                else:
                    send2trash(p)
            except Exception as e:
                ok = False
                errors.append((p, str(e)))
            try:
                delete_file_entry(self.conn, p)
            except Exception as e:
                # log but continue
                errors.append((p, f"db: {e}"))
            log_operation(self.conn, "delete" if (permanent or not self.settings.use_trash) else "trash", p, None, json.dumps({"permanent": permanent, "use_trash": self.settings.use_trash}), ok)
        if errors:
            QtWidgets.QMessageBox.warning(self, "Some errors", "\n".join([f"{p}: {err}" for p, err in errors[:10]]))
        self._refresh_library()
        self._refresh_duplicates()

    def _act_trash_selected(self):
        rows = self._dup_selected_rows()
        if not rows:
            return
        paths = [r["path"] for r in rows]
        if QtWidgets.QMessageBox.question(self, "Move to Trash", f"Move {len(paths)} selected files to trash?") != QtWidgets.QMessageBox.Yes:
            return
        self._perform_delete(paths, permanent=False)

    def _act_delete_selected(self):
        rows = self._dup_selected_rows()
        if not rows:
            return
        paths = [r["path"] for r in rows]
        if QtWidgets.QMessageBox.question(self, "Permanent Delete", f"PERMANENTLY delete {len(paths)} selected files? This cannot be undone.") != QtWidgets.QMessageBox.Yes:
            return
        self._perform_delete(paths, permanent=True)

    def _act_keep_trash_others(self):
        # Keep selected; for each selected group, trash all non-selected paths in the same group
        rows = self._dup_selected_rows()
        if not rows:
            return
        # build mapping group -> set(paths_to_keep)
        keep_by_group = {}
        for r in rows:
            keep_by_group.setdefault(r["group"], set()).add(r["path"])
        # collect candidates by scanning current model
        to_trash = []
        for row in range(self.dup_model.rowCount()):
            group_key = self.dup_model.item(row, 0).text()
            path = self.dup_model.item(row, 1).text()
            if group_key in keep_by_group and path not in keep_by_group[group_key]:
                to_trash.append(path)
        if not to_trash:
            QtWidgets.QMessageBox.information(self, "Nothing to trash", "No other files in the selected duplicate groups.")
            return
        if QtWidgets.QMessageBox.question(self, "Trash Others", f"Move {len(to_trash)} files to trash, keeping {len(rows)} selected?") != QtWidgets.QMessageBox.Yes:
            return
        self._perform_delete(to_trash, permanent=False)

    # Organizer Tab
    def _init_organizer_tab(self):
        w = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(w)

        hint = QtWidgets.QLabel("Select a folder to analyze. Parsed media info will be shown with proposed targets.")
        layout.addWidget(hint)

        path_layout = QtWidgets.QHBoxLayout()
        self.org_path_edit = QtWidgets.QLineEdit()
        btn_browse = QtWidgets.QPushButton("Browse")
        btn_browse.clicked.connect(self._browse_org_root)
        path_layout.addWidget(self.org_path_edit)
        path_layout.addWidget(btn_browse)
        layout.addLayout(path_layout)

        self.org_table = QtWidgets.QTableView()
        layout.addWidget(self.org_table)
        self._setup_org_model()

        controls = QtWidgets.QHBoxLayout()
        self.btn_preview = QtWidgets.QPushButton("Analyze Folder")
        self.btn_preview.clicked.connect(self._preview_org)
        self.btn_apply = QtWidgets.QPushButton("Apply (Rename/Move)")
        self.btn_apply.setEnabled(False)
        self.btn_apply.setToolTip("Disabled while duplicates are present in the view")
        controls.addWidget(self.btn_preview)
        controls.addWidget(self.btn_apply)
        layout.addLayout(controls)

        self.tabs.addTab(w, "Organizer")

    def _browse_org_root(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Folder")
        if d:
            self.org_path_edit.setText(d)

    def _setup_org_model(self):
        from PySide6.QtGui import QStandardItemModel
        self.org_model = QStandardItemModel()
        headers = ["Select", "Path", "Show", "Season", "Episode", "Episode2", "Proposed Target"]
        self.org_model.setHorizontalHeaderLabels(headers)
        self.org_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.org_table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.org_table.setModel(self.org_model)
        org_header = self.org_table.horizontalHeader()
        org_header.setStretchLastSection(False)
        org_header.setMinimumSectionSize(40)
        for i in range(self.org_model.columnCount()):
            org_header.setSectionResizeMode(i, QtWidgets.QHeaderView.Interactive)
        # Narrow the Select column and widen Path
        if self.org_model.columnCount() > 0:
            self.org_table.setColumnWidth(0, 40)  # Select
        if self.org_model.columnCount() > 1:
            self.org_table.setColumnWidth(1, 500)  # Path

    def _preview_org(self):
        root = self.org_path_edit.text().strip()
        self.org_model.removeRows(0, self.org_model.rowCount())
        self.btn_apply.setEnabled(False)
        from PySide6.QtCore import Qt
        from ..organizer import parse_filename, parse_from_path, propose_path
        from PySide6.QtGui import QStandardItem
        from ..db import fetch_library_rows, fetch_duplicate_rows
        # Build a map of path -> group_key for duplicates (include suspected)
        dup_rows = fetch_duplicate_rows(self.conn, include_suspected=True)
        dup_map = {str(p): g for (g, p, *_rest) in dup_rows}
        rows_source = []
        rows_source = fetch_library_rows(self.conn)
        if root:
            rows_source = [r for r in rows_source if root.lower() in str(r[0]).lower()]
        for row in rows_source:
            path = Path(row[0])
            parsed = parse_filename(path.name) or parse_from_path(path)
            show = season = episode = episode2 = ""
            target = ""
            if parsed:
                show = parsed.show
                season = f"{parsed.season:02d}"
                episode = f"{parsed.episode:02d}"
                episode2 = f"{parsed.episode2:02d}" if parsed.episode2 else ""
                target = str(propose_path(path, parsed))
            sel_item = QStandardItem("")
            sel_item.setCheckable(True)
            sel_item.setCheckState(Qt.Unchecked)
            row_items = [sel_item] + [QStandardItem(str(x)) for x in [str(path), show, season, episode, episode2, target]]
            # Highlight duplicates using same group color as Duplicates tab
            gkey = dup_map.get(str(path))
            if gkey:
                # Strong red background for duplicates in organizer
                from PySide6.QtGui import QColor
                bg = QColor(255, 80, 80, 120)
                for it in row_items:
                    it.setBackground(bg)
                self.btn_apply.setEnabled(False)
            self.org_model.appendRow(row_items)

    # Settings Tab
    def _init_junk_tab(self):
        from .junk_tab import JunkTab
        w = JunkTab(self, self.conn, self.settings)
        self.tabs.addTab(w, "Junk")

        # Unknown Types Tab
        from .unknown_tab import UnknownTab
        w = UnknownTab(self, self.conn, self.settings)
        self.tabs.addTab(w, "Unknown Types")

    def _init_settings_tab(self):
        w = QtWidgets.QWidget()
        vlayout = QtWidgets.QVBoxLayout(w)
        layout = QtWidgets.QFormLayout()

        # Hash algorithm dropdown with speed hints
        self.combo_hash = QtWidgets.QComboBox()
        items = [
            ("blake3", "blake3 — fastest (CPU-efficient)"),
            ("xxhash64", "xxhash64 — very fast (non-cryptographic)"),
            ("sha256", "sha256 — slowest (built-in)")
        ]
        for key, label in items:
            self.combo_hash.addItem(label, userData=key)
        # set current by matching userData
        idx = next((i for i in range(self.combo_hash.count()) if self.combo_hash.itemData(i) == self.settings.hash_algo), 0)
        self.combo_hash.setCurrentIndex(idx)
        self.combo_hash.currentIndexChanged.connect(self._on_hash_algo_idx)
        # Force default to xxhash64 on first run
        for i in range(self.combo_hash.count()):
            if self.combo_hash.itemData(i) == 'xxhash64':
                self.combo_hash.setCurrentIndex(i)
                break

        # sample size
        self.spin_sample = QtWidgets.QSpinBox()
        self.spin_sample.setRange(0, 1024 * 1024 * 1024)
        self.spin_sample.setSingleStep(1024 * 1024)
        self.spin_sample.setValue(self.settings.hash_sample_size)
        self.spin_sample.valueChanged.connect(self._on_sample_size)

        # do full hash
        self.chk_fullhash = QtWidgets.QCheckBox("Compute full-file hash")
        self.chk_fullhash.setChecked(self.settings.do_full_hash)
        self.chk_fullhash.stateChanged.connect(self._on_fullhash)

        # skip unchanged
        self.chk_skip = QtWidgets.QCheckBox("Skip unchanged files (size/mtime/inode)")
        self.chk_skip.setChecked(self.settings.skip_unchanged)
        self.chk_skip.stateChanged.connect(self._on_skip)

        # max workers
        self.spin_workers = QtWidgets.QSpinBox()
        self.spin_workers.setRange(1, 64)
        self.spin_workers.setValue(self.settings.max_workers)
        self.spin_workers.valueChanged.connect(self._on_workers)

        # use trash
        self.chk_trash = QtWidgets.QCheckBox("Move to trash instead of delete")
        self.chk_trash.setChecked(self.settings.use_trash)
        self.chk_trash.stateChanged.connect(self._on_trash)

        layout.addRow("Hash Algorithm", self.combo_hash)
        layout.addRow("Sample Size (bytes)", self.spin_sample)
        layout.addRow(self.chk_fullhash)
        layout.addRow(self.chk_skip)
        layout.addRow("Max Workers", self.spin_workers)
        layout.addRow(self.chk_trash)

        # Media/Junk settings editors
        media_group = QtWidgets.QGroupBox("Media and Junk Settings")
        media_layout = QtWidgets.QFormLayout(media_group)
        self.edit_media_exts = QtWidgets.QLineEdit(", ".join(self.settings.media_extensions))
        self.edit_image_exts = QtWidgets.QLineEdit(", ".join(getattr(self.settings, 'image_extensions', [])))
        self.edit_subtitle_exts = QtWidgets.QLineEdit(", ".join(getattr(self.settings, 'subtitle_extensions', [])))
        self.edit_xml_exts = QtWidgets.QLineEdit(", ".join(getattr(self.settings, 'xml_extensions', [])))
        self.edit_junk_patterns = QtWidgets.QLineEdit(", ".join(self.settings.junk_patterns))
        self.edit_junk_exclude = QtWidgets.QLineEdit(", ".join(self.settings.junk_exclude_extensions))
        media_layout.addRow("Video extensions (comma-separated)", self.edit_media_exts)
        media_layout.addRow("Image extensions (comma-separated)", self.edit_image_exts)
        media_layout.addRow("Subtitle extensions (comma-separated)", self.edit_subtitle_exts)
        media_layout.addRow("XML/NFO extensions (comma-separated)", self.edit_xml_exts)
        self.edit_other_exts = QtWidgets.QLineEdit(", ".join(getattr(self.settings, 'other_extensions', [])))
        media_layout.addRow("Other extensions (comma-separated)", self.edit_other_exts)
        media_layout.addRow("Junk patterns (glob, comma-separated)", self.edit_junk_patterns)
        media_layout.addRow("Junk exclude extensions", self.edit_junk_exclude)
        vlayout.addWidget(media_group)

        # Agent performance settings
        agent_group = QtWidgets.QGroupBox("Agent Performance")
        agent_layout = QtWidgets.QFormLayout(agent_group)
        self.spin_agent_workers = QtWidgets.QSpinBox()
        self.spin_agent_workers.setRange(1, 64)
        self.spin_agent_workers.setValue(getattr(self.settings, 'agent_max_workers', 4))
        self.spin_agent_workers.valueChanged.connect(lambda v: setattr(self.settings, 'agent_max_workers', int(v)))
        self.spin_agent_batch = QtWidgets.QSpinBox()
        self.spin_agent_batch.setRange(10, 10000)
        self.spin_agent_batch.setSingleStep(50)
        self.spin_agent_batch.setValue(getattr(self.settings, 'agent_batch_size', 500))
        self.spin_agent_batch.valueChanged.connect(lambda v: setattr(self.settings, 'agent_batch_size', int(v)))
        agent_layout.addRow("Agent max workers", self.spin_agent_workers)
        agent_layout.addRow("Agent batch size", self.spin_agent_batch)
        vlayout.addWidget(agent_group)

        # Roots manager
        roots_group = QtWidgets.QGroupBox("Library Roots")
        roots_layout = QtWidgets.QVBoxLayout(roots_group)

        add_layout = QtWidgets.QHBoxLayout()
        self.root_path_edit = QtWidgets.QLineEdit()
        btn_browse_root = QtWidgets.QPushButton("Browse")
        btn_browse_root.clicked.connect(self._browse_root_add)
        btn_add_root = QtWidgets.QPushButton("Add Root")
        btn_add_root.clicked.connect(self._add_root)
        add_layout.addWidget(self.root_path_edit)
        add_layout.addWidget(btn_browse_root)
        add_layout.addWidget(btn_add_root)

        self.roots_list = QtWidgets.QListWidget()
        self.roots_list.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)

        btns_layout = QtWidgets.QHBoxLayout()
        self.btn_enable_root = QtWidgets.QPushButton("Enable Selected")
        self.btn_enable_root.clicked.connect(self._enable_selected_root)
        self.btn_disable_root = QtWidgets.QPushButton("Disable Selected")
        self.btn_disable_root.clicked.connect(self._disable_selected_root)
        self.btn_remove_root = QtWidgets.QPushButton("Remove Root")
        self.btn_remove_root.clicked.connect(self._remove_selected_root)
        self.btn_clear_root = QtWidgets.QPushButton("Clear Library for Root")
        self.btn_clear_root.clicked.connect(self._clear_selected_root)
        btns_layout.addWidget(self.btn_enable_root)
        btns_layout.addWidget(self.btn_disable_root)
        btns_layout.addWidget(self.btn_remove_root)
        btns_layout.addWidget(self.btn_clear_root)

        roots_layout.addLayout(add_layout)
        roots_layout.addWidget(self.roots_list)
        roots_layout.addLayout(btns_layout)

        # Reset entire library
        reset_layout = QtWidgets.QHBoxLayout()
        self.btn_reset_lib = QtWidgets.QPushButton("Reset Library (All Entries)")
        self.btn_reset_lib.clicked.connect(self._reset_library)
        reset_layout.addStretch(1)
        reset_layout.addWidget(self.btn_reset_lib)
        roots_layout.addLayout(reset_layout)

        # Organizer naming settings
        org_group = QtWidgets.QGroupBox("Organizer Naming Pattern")
        org_layout = QtWidgets.QVBoxLayout(org_group)
        pattern_help = QtWidgets.QLabel("Use tokens: {show}, {season:02d}, {episode:02d}, {title}. Example: '{show} - S{season:02d}E{episode:02d} - {title}'")
        self.edit_pattern = QtWidgets.QLineEdit()
        # Initialize from settings if exists, else default
        try:
            from ..settings import Settings as _S
            tmpl = getattr(self.settings, 'naming_template', '{show} - S{season:02d}E{episode:02d}')
        except Exception:
            tmpl = '{show} - S{season:02d}E{episode:02d}'
        self.edit_pattern.setText(tmpl)
        self.edit_pattern.textChanged.connect(self._on_pattern_changed)
        btn_reset_pattern = QtWidgets.QPushButton("Reset Default Pattern")
        def _reset_pat():
            self.edit_pattern.setText('{show} - S{season:02d}E{episode:02d}')
        btn_reset_pattern.clicked.connect(_reset_pat)
        org_layout.addWidget(pattern_help)
        org_layout.addWidget(self.edit_pattern)
        org_layout.addWidget(btn_reset_pattern)

        # Pattern builder UI
        builder_layout = QtWidgets.QVBoxLayout()
        row1 = QtWidgets.QHBoxLayout()
        self.combo_pattern_token = QtWidgets.QComboBox()
        self.combo_pattern_token.addItems(["Show", "Season", "Episode", "Title", "Separator", "Literal text..."])
        btn_add_token = QtWidgets.QPushButton("Add")
        btn_add_token.clicked.connect(self._add_pattern_token)
        row1.addWidget(self.combo_pattern_token)
        row1.addWidget(btn_add_token)
        builder_layout.addLayout(row1)

        self.pattern_list = QtWidgets.QListWidget()
        self.pattern_list.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        builder_layout.addWidget(self.pattern_list)

        row2 = QtWidgets.QHBoxLayout()
        btn_up = QtWidgets.QPushButton("Move Up")
        btn_down = QtWidgets.QPushButton("Move Down")
        btn_remove = QtWidgets.QPushButton("Remove")
        btn_up.clicked.connect(lambda: self._move_pattern_item(-1))
        btn_down.clicked.connect(lambda: self._move_pattern_item(1))
        btn_remove.clicked.connect(self._remove_pattern_item)
        row2.addWidget(btn_up)
        row2.addWidget(btn_down)
        row2.addWidget(btn_remove)
        builder_layout.addLayout(row2)

        self.label_preview = QtWidgets.QLabel("Preview: ")
        builder_layout.addWidget(self.label_preview)
        org_layout.addLayout(builder_layout)

        vlayout.addLayout(layout)
        vlayout.addWidget(roots_group)
        # Settings footer actions (visible only on Settings tab)
        footer = QtWidgets.QHBoxLayout()

        # Create progress bars before adding to footer
        self.pb_local = QtWidgets.QProgressBar()
        self.pb_local.setFormat("Local scan in progress...")
        self.pb_local.setTextVisible(True)
        self.pb_local.setRange(0, 0)
        self.pb_local.setVisible(False)

        self.pb_remote = QtWidgets.QProgressBar()
        self.pb_remote.setFormat("Remote scan in progress...")
        self.pb_remote.setTextVisible(True)
        self.pb_remote.setRange(0, 0)
        self.pb_remote.setVisible(False)

        self.pb_remote_video = QtWidgets.QProgressBar()
        self.pb_remote_video.setFormat("Remote video in progress...")
        self.pb_remote_video.setTextVisible(True)
        self.pb_remote_video.setRange(0, 0)
        self.pb_remote_video.setVisible(False)

        btn_scan_local = QtWidgets.QPushButton("Scan Local Roots")
        btn_scan_local.clicked.connect(self._start_local_roots_scan)
        btn_scan_remote = QtWidgets.QPushButton("Start Remote Scan")
        btn_scan_remote.clicked.connect(self._start_remote_scan)
        btn_cache_info = QtWidgets.QPushButton("Agent Cache Info")
        btn_cache_info.clicked.connect(self._show_agent_cache_info)
        btn_cache_clear = QtWidgets.QPushButton("Clear Agent Cache")
        btn_cache_clear.clicked.connect(self._clear_agent_cache)
        btn_reset_all = QtWidgets.QPushButton("Reset Library (All Entries)")
        btn_reset_all.clicked.connect(self._reset_library)

        footer.addWidget(btn_scan_local)
        footer.addWidget(self.pb_local)
        footer.addWidget(btn_scan_remote)
        footer.addWidget(self.pb_remote)
        footer.addWidget(self.pb_remote_video)
        footer.addStretch(1)
        footer.addWidget(btn_cache_info)
        footer.addWidget(btn_cache_clear)
        footer.addWidget(btn_reset_all)

        vlayout.addLayout(footer)
        # Ingestion server controls
        ingest_group = QtWidgets.QGroupBox("Ingestion Server")
        ingest_layout = QtWidgets.QHBoxLayout(ingest_group)
        self.lbl_ingest = QtWidgets.QLabel("Stopped")
        self.btn_start_ingest = QtWidgets.QPushButton("Start Server")
        self.btn_start_ingest.clicked.connect(self._start_ingest_server)
        self.btn_stop_ingest = QtWidgets.QPushButton("Stop Server")
        self.btn_stop_ingest.clicked.connect(self._stop_ingest_server)
        self.btn_refresh_ingest = QtWidgets.QPushButton("Refresh Status")
        self.btn_refresh_ingest.clicked.connect(self._refresh_ingest_status)
        self.btn_check_ingest = QtWidgets.QPushButton("Check Server URL")
        self.btn_check_ingest.clicked.connect(self._check_ingest_url)
        ingest_layout.addWidget(self.lbl_ingest)
        ingest_layout.addWidget(self.btn_start_ingest)
        ingest_layout.addWidget(self.btn_stop_ingest)
        ingest_layout.addWidget(self.btn_refresh_ingest)
        ingest_layout.addWidget(self.btn_check_ingest)

        # Remote Library Roots (agent-powered browsing)
        remote_group = QtWidgets.QGroupBox("Remote Library Roots (via Agent)")
        remote_layout = QtWidgets.QVBoxLayout(remote_group)
        row = QtWidgets.QHBoxLayout()
        self.edit_agent_ip = QtWidgets.QLineEdit()
        self.edit_agent_ip.setPlaceholderText("Agent IP (e.g., 192.168.10.50)")
        btn_browse_remote = QtWidgets.QPushButton("Browse Remote")
        btn_browse_remote.clicked.connect(self._browse_remote)
        row.addWidget(self.edit_agent_ip)
        row.addWidget(btn_browse_remote)
        remote_layout.addLayout(row)

        self.remote_roots_list = QtWidgets.QListWidget()
        remote_layout.addWidget(self.remote_roots_list)

        btns = QtWidgets.QHBoxLayout()
        btn_remove_remote = QtWidgets.QPushButton("Remove Selected Remote Root")
        btn_remove_remote.clicked.connect(self._remove_selected_remote_root)
        btns.addWidget(btn_remove_remote)
        remote_layout.addLayout(btns)

        # Build Settings sub-tabs
        self.settings_tabs = QtWidgets.QTabWidget()

        # Organizer sub-tab
        tab_org = QtWidgets.QWidget()
        tab_org_layout = QtWidgets.QVBoxLayout(tab_org)
        tab_org_layout.addWidget(org_group)
        self.settings_tabs.addTab(tab_org, "Organizer")

        # Local Roots sub-tab
        tab_local = QtWidgets.QWidget()
        tab_local_layout = QtWidgets.QVBoxLayout(tab_local)
        tab_local_layout.addWidget(roots_group)
        self.settings_tabs.addTab(tab_local, "Local Roots")

        # Remote & Server sub-tab
        tab_remote = QtWidgets.QWidget()
        tab_remote_layout = QtWidgets.QVBoxLayout(tab_remote)
        # Agent ping controls
        ping_row = QtWidgets.QHBoxLayout()
        self.btn_ping_agent = QtWidgets.QPushButton("Ping Agent")
        self.btn_ping_agent.clicked.connect(self._ping_agent)
        ping_row.addWidget(self.btn_ping_agent)
        tab_remote_layout.addLayout(ping_row)

        tab_remote_layout.addWidget(remote_group)
        tab_remote_layout.addWidget(ingest_group)
        self.settings_tabs.addTab(tab_remote, "Remote & Server")

        # Add the sub-tabs into the Settings tab
        vlayout.addWidget(media_group)
        vlayout.addWidget(self.settings_tabs)

        self.tabs.addTab(w, "Settings")
        # refresh lists
        self._refresh_remote_roots()

    def _reset_library(self):
        from ..db import clear_all_library, count_all_files
        count = count_all_files(self.conn)
        if count == 0:
            QtWidgets.QMessageBox.information(self, "Reset Library", "Library is already empty.")
            return
        if QtWidgets.QMessageBox.question(self, "Reset Library", f"This will remove ALL {count} indexed entries across all roots. Continue?\n(Files on disk are not touched.)") == QtWidgets.QMessageBox.Yes:
            clear_all_library(self.conn)
            # Also update junk/unknown related views
            try:
                # Refresh Junk tab if present
                self._update_junk_status()
            except Exception:
                pass
            self._refresh_library()
            self._refresh_duplicates()
            QtWidgets.QMessageBox.information(self, "Reset Library", "Library and junk entries have been cleared.")

    # Ingestion server controls
    def _start_ingest_server(self):
        try:
            from ..ingest_server import start_server
            start_server(self.settings)
            self._refresh_ingest_status()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Ingestion Server", f"Failed to start server: {e}")

    def _stop_ingest_server(self):
        try:
            from ..ingest_server import stop_server
            stop_server()
            # Give the server a moment to shut down
            QtCore.QTimer.singleShot(500, self._refresh_ingest_status)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Ingestion Server", f"Failed to stop server: {e}")

    def _save_prefs(self):
        import json, os
        from pathlib import Path as _P
        data = {
            "agent_ip": self.edit_agent_ip.text().strip() if hasattr(self, 'edit_agent_ip') else "",
            "hash_algo": getattr(self.settings, 'hash_algo', None),
            "hash_sample_size": getattr(self.settings, 'hash_sample_size', None),
            "do_full_hash": getattr(self.settings, 'do_full_hash', None),
            "skip_unchanged": getattr(self.settings, 'skip_unchanged', None),
            "max_workers": getattr(self.settings, 'max_workers', None),
            "use_trash": getattr(self.settings, 'use_trash', None),
            "agent_max_workers": getattr(self.settings, 'agent_max_workers', None),
            "agent_batch_size": getattr(self.settings, 'agent_batch_size', None),
            "media_extensions": getattr(self.settings, 'media_extensions', []),
            "image_extensions": getattr(self.settings, 'image_extensions', []),
            "subtitle_extensions": getattr(self.settings, 'subtitle_extensions', []),
            "xml_extensions": getattr(self.settings, 'xml_extensions', []),
            "other_extensions": getattr(self.settings, 'other_extensions', []),
            "junk_patterns": getattr(self.settings, 'junk_patterns', []),
            "junk_exclude_extensions": getattr(self.settings, 'junk_exclude_extensions', []),
            "naming_template": getattr(self.settings, 'naming_template', ''),
        }
        prefs_path = _P.home() / ".medialib" / "ui_prefs.json"
        prefs_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            prefs_path.write_text(json.dumps(data, indent=2))
        except Exception:
            pass

    def _load_prefs(self):
        import json
        from pathlib import Path as _P
        prefs_path = _P.home() / ".medialib" / "ui_prefs.json"
        if not prefs_path.exists():
            return
        data = json.loads(prefs_path.read_text())
        # restore agent ip
        if hasattr(self, 'edit_agent_ip'):
            self.edit_agent_ip.setText(str(data.get('agent_ip','')))
        # update settings
        for k in [
            'hash_algo','hash_sample_size','do_full_hash','skip_unchanged','max_workers','use_trash',
            'agent_max_workers','agent_batch_size','media_extensions','image_extensions','subtitle_extensions','xml_extensions','other_extensions','junk_patterns','junk_exclude_extensions','naming_template']:
            v = data.get(k)
            if v is not None:
                setattr(self.settings, k, v)
        # also update UI controls to reflect any changes
        try:
            # hash algo combo
            for i in range(self.combo_hash.count()):
                if self.combo_hash.itemData(i) == self.settings.hash_algo:
                    self.combo_hash.setCurrentIndex(i)
                    break
            self.spin_sample.setValue(int(self.settings.hash_sample_size))
            self.chk_fullhash.setChecked(bool(self.settings.do_full_hash))
            self.chk_skip.setChecked(bool(self.settings.skip_unchanged))
            self.spin_workers.setValue(int(self.settings.max_workers))
            self.chk_trash.setChecked(bool(self.settings.use_trash))
            self.spin_agent_workers.setValue(int(getattr(self.settings,'agent_max_workers',4)))
            self.spin_agent_batch.setValue(int(getattr(self.settings,'agent_batch_size',500)))
            self.edit_media_exts.setText(", ".join(self.settings.media_extensions))
            self.edit_image_exts.setText(", ".join(getattr(self.settings,'image_extensions',[])))
            self.edit_subtitle_exts.setText(", ".join(getattr(self.settings,'subtitle_extensions',[])))
            self.edit_xml_exts.setText(", ".join(getattr(self.settings,'xml_extensions',[])))
            self.edit_other_exts.setText(", ".join(getattr(self.settings,'other_extensions',[])))
            self.edit_junk_patterns.setText(", ".join(self.settings.junk_patterns))
            self.edit_junk_exclude.setText(", ".join(self.settings.junk_exclude_extensions))
            self.edit_pattern.setText(getattr(self.settings,'naming_template',''))
            self._update_pattern_preview()
        except Exception:
            pass

    def _refresh_remote_roots(self):
        from ..db import list_remote_roots
        self.remote_roots_list.clear()
        for p in list_remote_roots(self.conn):
            self.remote_roots_list.addItem(p)

    def _remove_selected_remote_root(self):
        from ..db import remove_remote_root
        item = self.remote_roots_list.currentItem()
        if not item:
            return
        path = item.text()
        if QtWidgets.QMessageBox.question(self, "Remove Remote Root", f"Remove '{path}'?") == QtWidgets.QMessageBox.Yes:
            remove_remote_root(self.conn, path)
            self._refresh_remote_roots()

    def _browse_remote(self):
        ip = self.edit_agent_ip.text().strip()
        if not ip:
            QtWidgets.QMessageBox.warning(self, "Agent IP missing", "Enter the Agent IP address.")
            return
        url_base = f"http://{ip}:8877"
        # simple dialog for remote browsing
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Browse Remote Agent")
        v = QtWidgets.QVBoxLayout(dlg)
        path_bar = QtWidgets.QHBoxLayout()
        edit_path = QtWidgets.QLineEdit("/")
        btn_up = QtWidgets.QPushButton("Up")
        btn_open = QtWidgets.QPushButton("Open")
        path_bar.addWidget(edit_path)
        path_bar.addWidget(btn_up)
        path_bar.addWidget(btn_open)
        v.addLayout(path_bar)
        listw = QtWidgets.QListWidget()
        v.addWidget(listw)
        btns = QtWidgets.QHBoxLayout()
        btn_add_current = QtWidgets.QPushButton("Add This Folder")
        btn_cancel = QtWidgets.QPushButton("Cancel")
        btns.addWidget(btn_add_current)
        btns.addWidget(btn_cancel)
        v.addLayout(btns)

        def load_path(pth: str):
            try:
                import requests
                r = requests.get(f"{url_base}/agent/ls", params={"path": pth}, timeout=10)
                if r.status_code != 200:
                    QtWidgets.QMessageBox.warning(dlg, "Browse", f"Error: {r.text}")
                    return
                data = r.json()
                edit_path.setText(data.get("path", pth))
                listw.clear()
                for d in data.get("dirs", []):
                    # show directory name but store full POSIX path as data
                    item = QtWidgets.QListWidgetItem(d["name"]) 
                    item.setData(QtCore.Qt.UserRole, d["path"]) 
                    listw.addItem(item)
            except Exception as e:
                QtWidgets.QMessageBox.warning(dlg, "Browse", f"Failed: {e}")

        def go_up():
            from pathlib import Path as _P
            # POSIX-safe parent computation without converting separators
            txt = edit_path.text().strip()
            if not txt or txt == "/":
                load_path("/")
                return
            parts = txt.rstrip("/").split("/")
            parent = "/" if len(parts) <= 1 else "/" + "/".join(parts[:-1])
            load_path(parent)

        def open_selected():
            item = listw.currentItem()
            if not item:
                return
            pth = item.data(QtCore.Qt.UserRole)
            if pth:
                load_path(str(pth))

        def add_current():
            path = edit_path.text().strip()
            if not path:
                return
            from ..db import add_remote_root
            add_remote_root(self.conn, path)
            self._refresh_remote_roots()
            dlg.accept()

        btn_up.clicked.connect(go_up)
        btn_open.clicked.connect(open_selected)
        btn_cancel.clicked.connect(dlg.reject)
        btn_add_current.clicked.connect(add_current)
        listw.itemDoubleClicked.connect(lambda _: open_selected())

        load_path("/")
        dlg.exec()

    def _ping_agent(self):
        import requests
        ip = self.edit_agent_ip.text().strip()
        if not ip:
            QtWidgets.QMessageBox.warning(self, "Agent IP missing", "Enter the Agent IP address.")
            return
        url = f"http://{ip}:8877/agent/ping"
        try:
            r = requests.get(url, timeout=5)
            if r.ok and r.json().get('ok'):
                QtWidgets.QMessageBox.information(self, "Agent Ping", f"Agent reachable at {url}")
            else:
                QtWidgets.QMessageBox.warning(self, "Agent Ping", f"Agent responded with {r.status_code}: {r.text}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Agent Ping", f"Failed to reach {url}: {e}")

    def _refresh_ingest_status(self):
        try:
            from ..ingest_server import get_ingest_stats
            stats = get_ingest_stats()
            if stats.get("running"):
                last_ts = stats.get("last_ingest_ts")
                cnt = stats.get("last_ingest_count", 0)
                ts_str = "never" if not last_ts else QtCore.QDateTime.fromSecsSinceEpoch(int(last_ts)).toString()
                self.lbl_ingest.setText(f"Running on {getattr(self.settings,'ingest_host','0.0.0.0')}:{getattr(self.settings,'ingest_port',8765)} | last: {ts_str} | total: {cnt}")
            else:
                self.lbl_ingest.setText("Stopped")
        except Exception as e:
            self.lbl_ingest.setText(f"Error: {e}")

    def closeEvent(self, event):
        try:
            self._save_prefs()
        except Exception:
            pass
        super().closeEvent(event)

    def _check_ingest_url(self):
        import socket, requests
        host = getattr(self.settings, 'ingest_host', '0.0.0.0')
        port = getattr(self.settings, 'ingest_port', 8765)
        candidates = []
        if host in ('0.0.0.0', '::', '', None):
            # Probe localhost and all local IPv4 addresses
            candidates.append(f"http://127.0.0.1:{port}/health")
            try:
                hostname = socket.gethostname()
                addrs = set()
                for fam, _, _, _, sockaddr in socket.getaddrinfo(hostname, None):
                    if fam == socket.AF_INET:
                        ip = sockaddr[0]
                        if not ip.startswith('127.'):
                            addrs.add(ip)
                for ip in sorted(addrs):
                    candidates.append(f"http://{ip}:{port}/health")
            except Exception:
                pass
        else:
            candidates.append(f"http://{host}:{port}/health")
        ok_urls = []
        errors = []
        for url in candidates:
            try:
                r = requests.get(url, timeout=5)
                if r.ok and r.json().get('ok'):
                    ok_urls.append(url)
                else:
                    errors.append(f"{url} -> {r.status_code}")
            except Exception as e:
                errors.append(f"{url} -> {e}")
        if ok_urls:
            msg = "Server reachable at:\n" + "\n".join(ok_urls)
            QtWidgets.QMessageBox.information(self, "Server Health", msg)
        else:
            msg = "Server not reachable. Tried:\n" + "\n".join(candidates) + "\n\nErrors:\n" + "\n".join(errors[:5])
            QtWidgets.QMessageBox.critical(self, "Server Health", msg)

    def _on_hash_algo_idx(self, idx: int):
        algo = self.combo_hash.itemData(idx)
        if algo:
            self.settings.hash_algo = algo  # type: ignore

    def _on_pattern_changed(self, text: str):
        # Update settings and preview when user edits pattern directly
        self.settings.naming_template = text
        self._update_pattern_preview()

    def _update_pattern_preview(self):
        # Build a sample preview
        sample = {
            "show": "Sample Show",
            "season": 1,
            "episode": 2,
            "title": "Pilot",
        }
        try:
            preview = self.settings.naming_template.format(**sample)
        except Exception:
            preview = "(invalid pattern)"
        self.label_preview.setText(f"Preview: {preview}")

    # Pattern builder helpers
    def _add_pattern_token(self):
        kind = self.combo_pattern_token.currentText()
        if kind == "Show":
            text = "{show}"
        elif kind == "Season":
            text = "S{season:02d}"
        elif kind == "Episode":
            text = "E{episode:02d}"
        elif kind == "Title":
            text = "{title}"
        elif kind == "Separator":
            text = " - "
        else:
            txt, ok = QtWidgets.QInputDialog.getText(self, "Literal text", "Enter text:")
            if not ok:
                return
            text = txt
        self.pattern_list.addItem(text)
        self._sync_pattern_from_list()

    def _move_pattern_item(self, delta: int):
        row = self.pattern_list.currentRow()
        if row < 0:
            return
        new_row = max(0, min(self.pattern_list.count() - 1, row + delta))
        if new_row == row:
            return
        item = self.pattern_list.takeItem(row)
        self.pattern_list.insertItem(new_row, item)
        self.pattern_list.setCurrentRow(new_row)
        self._sync_pattern_from_list()

    def _remove_pattern_item(self):
        row = self.pattern_list.currentRow()
        if row >= 0:
            self.pattern_list.takeItem(row)
            self._sync_pattern_from_list()

    def _sync_pattern_from_list(self):
        # also save current media/junk settings
        self.settings.media_extensions = [s.strip() for s in self.edit_media_exts.text().split(',') if s.strip()]
        self.settings.image_extensions = [s.strip() for s in self.edit_image_exts.text().split(',') if s.strip()]
        self.settings.subtitle_extensions = [s.strip() for s in self.edit_subtitle_exts.text().split(',') if s.strip()]
        self.settings.xml_extensions = [s.strip() for s in self.edit_xml_exts.text().split(',') if s.strip()]
        # 'Other' extensions are curated via Unknown Types mapping; not editing here directly
        self.settings.junk_patterns = [s.strip() for s in self.edit_junk_patterns.text().split(',') if s.strip()]
        self.settings.junk_exclude_extensions = [s.strip() for s in self.edit_junk_exclude.text().split(',') if s.strip()]
        self.settings.other_extensions = [s.strip() for s in self.edit_other_exts.text().split(',') if s.strip()]
        parts = []
        for i in range(self.pattern_list.count()):
            parts.append(self.pattern_list.item(i).text())
        tmpl = "".join(parts)
        self.settings.naming_template = tmpl
        try:
            self._save_prefs()
        except Exception:
            pass
        # Avoid recursive signal loops: set text without signaling if needed
        self.edit_pattern.blockSignals(True)
        self.edit_pattern.setText(tmpl)
        self.edit_pattern.blockSignals(False)
        self._update_pattern_preview()

    def _on_sample_size(self, val: int):
        self.settings.hash_sample_size = int(val)

    def _on_fullhash(self, state: int):
        self.settings.do_full_hash = state == QtCore.Qt.Checked

    def _on_trash(self, state: int):
        self.settings.use_trash = state == QtCore.Qt.Checked

    def _on_skip(self, state: int):
        self.settings.skip_unchanged = state == QtCore.Qt.Checked

    def _on_workers(self, val: int):
        self.settings.max_workers = int(val)
        try:
            self._save_prefs()
        except Exception:
            pass

    # Roots management
    def _browse_root_add(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Root Folder")
        if d:
            self.root_path_edit.setText(d)

    def _refresh_roots(self):
        from ..db import list_roots
        self.roots_list.clear()
        for path, enabled in list_roots(self.conn):
            item = QtWidgets.QListWidgetItem(path)
            item.setCheckState(QtCore.Qt.Checked if enabled else QtCore.Qt.Unchecked)
            self.roots_list.addItem(item)

    def _add_root(self):
        from ..db import add_root
        p = self.root_path_edit.text().strip()
        if not p:
            QtWidgets.QMessageBox.warning(self, "Missing path", "Please select a folder")
            return
        add_root(self.conn, Path(p), True)
        self._refresh_roots()
        # Do not change other roots' enabled state; only add/update this one

    def _enable_selected_root(self):
        from ..db import set_root_enabled
        item = self.roots_list.currentItem()
        if not item:
            return
        path = item.text()
        set_root_enabled(self.conn, Path(path), True)
        self._refresh_roots()

    def _disable_selected_root(self):
        from ..db import set_root_enabled
        item = self.roots_list.currentItem()
        if not item:
            return
        path = item.text()
        set_root_enabled(self.conn, Path(path), False)
        self._refresh_roots()

    def _remove_selected_root(self):
        from ..db import remove_root
        item = self.roots_list.currentItem()
        if not item:
            return
        path = item.text()
        if QtWidgets.QMessageBox.question(self, "Remove Root", f"Remove root '{path}' from configuration?\n(This does not delete library entries.)") == QtWidgets.QMessageBox.Yes:
            remove_root(self.conn, Path(path))
            self._refresh_roots()
            self._refresh_library()
            self._refresh_duplicates()

    def _clear_selected_root(self):
        from ..db import clear_root, count_files_under_root
        item = self.roots_list.currentItem()
        if not item:
            return
        path = item.text()
        count = count_files_under_root(self.conn, Path(path))
        if QtWidgets.QMessageBox.question(self, "Clear Library", f"Remove {count} indexed entries under '{path}'?\n(This does not touch your files on disk.)") == QtWidgets.QMessageBox.Yes:
            clear_root(self.conn, Path(path))
            # also refresh junk status as junk under the root is cleared too
            try:
                self._update_junk_status()
            except Exception:
                pass
            self._refresh_library()
            self._refresh_duplicates()
