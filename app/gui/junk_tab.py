from __future__ import annotations
from PySide6 import QtWidgets

from .. import db as dbm  # type: ignore

class JunkTab(QtWidgets.QWidget):
    def __init__(self, parent, conn, settings):
        super().__init__(parent)
        self.conn = conn
        self.settings = settings
        layout = QtWidgets.QVBoxLayout(self)
        toolbar = QtWidgets.QHBoxLayout()
        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Filter by path...")
        btn_reload = QtWidgets.QPushButton("Reload")
        btn_reload.clicked.connect(self.reload)
        btn_clear_all = QtWidgets.QPushButton("Clear All Junk")
        btn_clear_all.clicked.connect(self.clear_all)
        btn_trash = QtWidgets.QPushButton("Move Selected to Trash")
        btn_trash.clicked.connect(self.trash_selected)
        btn_delete = QtWidgets.QPushButton("Delete Selected (Permanent)")
        btn_delete.clicked.connect(self.delete_selected)
        self.total_label = QtWidgets.QLabel("Total size: 0 B")
        toolbar.addWidget(self.search)
        toolbar.addWidget(btn_reload)
        toolbar.addWidget(btn_clear_all)
        toolbar.addWidget(btn_trash)
        toolbar.addWidget(btn_delete)
        btn_promote = QtWidgets.QPushButton("Promote Selected to Other (Keep)")
        btn_promote.clicked.connect(self.promote_selected_to_other)
        toolbar.addWidget(btn_promote)
        toolbar.addWidget(self.total_label)
        layout.addLayout(toolbar)

        self.table = QtWidgets.QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Name", "Path", "Size", "Modified", "Ext", "Reason"])
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        self.reload()

    def clear_all(self):
        if QtWidgets.QMessageBox.question(self, "Clear All Junk", "Remove all junk entries from the database?\n(Files on disk are not touched.)") != QtWidgets.QMessageBox.Yes:
            return
        try:
            dbm.clear_all_junk(self.conn)
        except Exception:
            pass
        self.reload()

    def promote_selected_to_other(self):
        paths = self._selected_paths()
        if not paths:
            return
        if QtWidgets.QMessageBox.question(self, "Promote to Other", f"Promote {len(paths)} entries to 'Other' and remove from Junk?\nTheir extensions will be added to Other extensions.") != QtWidgets.QMessageBox.Yes:
            return
        # Collect unique extensions from selected rows
        exts = set()
        for idx in self.table.selectionModel().selectedRows():
            ext = self.table.item(idx.row(), 3).text().strip().lower()
            if ext:
                exts.add(ext)
        # Update settings.other_extensions list
        current = set(x.lower() for x in getattr(self.settings, 'other_extensions', []))
        for e in exts:
            if e not in current:
                self.settings.other_extensions.append(e)
        # Remove junk entries from DB
        for p in paths:
            try:
                dbm.delete_junk_entry(self.conn, p)
            except Exception:
                pass
        QtWidgets.QMessageBox.information(self, "Promoted", f"Promoted extensions: {', '.join(sorted(exts))}\nEntries removed from Junk.")
        self.reload()

    def reload(self):
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
        rows = dbm.list_junk(self.conn)
        q = self.search.text().strip().lower()
        if q:
            rows = [r for r in rows if q in str(r[0]).lower()]
        total = 0
        self.table.setRowCount(0)
        from pathlib import Path as _P
        for path, size, mtime, ext, reason in rows:
            total += int(size or 0)
            r = self.table.rowCount()
            self.table.insertRow(r)
            name = _P(str(path)).name
            self.table.setItem(r, 0, QtWidgets.QTableWidgetItem(name))
            self.table.setItem(r, 1, QtWidgets.QTableWidgetItem(str(path)))
            self.table.setItem(r, 2, QtWidgets.QTableWidgetItem(_human_size(size)))
            self.table.setItem(r, 3, QtWidgets.QTableWidgetItem(_human_time(mtime)))
            self.table.setItem(r, 4, QtWidgets.QTableWidgetItem(str(ext or "")))
            self.table.setItem(r, 5, QtWidgets.QTableWidgetItem(str(reason or "")))
        self.total_label.setText(f"Total size: {_human_size(total)}")

    def _selected_paths(self):
        paths = []
        for idx in self.table.selectionModel().selectedRows():
            p = self.table.item(idx.row(), 0).text()
            paths.append(p)
        return paths

    def trash_selected(self):
        from send2trash import send2trash
        import os
        from . import db as dbm
        paths = self._selected_paths()
        if not paths:
            return
        if QtWidgets.QMessageBox.question(self, "Move to Trash", f"Move {len(paths)} files to trash?") != QtWidgets.QMessageBox.Yes:
            return
        for p in paths:
            try:
                if self.settings.use_trash:
                    send2trash(p)
                else:
                    os.remove(p)
                dbm.delete_junk_entry(self.conn, p)
            except Exception:
                pass
        self.reload()

    def delete_selected(self):
        import os
        from . import db as dbm
        paths = self._selected_paths()
        if not paths:
            return
        if QtWidgets.QMessageBox.question(self, "Permanent Delete", f"Permanently delete {len(paths)} files?") != QtWidgets.QMessageBox.Yes:
            return
        for p in paths:
            try:
                os.remove(p)
                dbm.delete_junk_entry(self.conn, p)
            except Exception:
                pass
        self.reload()
