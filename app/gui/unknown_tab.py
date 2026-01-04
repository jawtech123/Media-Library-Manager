from __future__ import annotations
from PySide6 import QtWidgets
from typing import List, Tuple

from .. import db as dbm  # type: ignore

CATEGORIES = ["video", "image", "subtitle", "xml", "other", "unknown"]

class UnknownTab(QtWidgets.QWidget):
    def __init__(self, parent, conn, settings):
        super().__init__(parent)
        self.conn = conn
        self.settings = settings
        layout = QtWidgets.QVBoxLayout(self)

        # Toolbar
        toolbar = QtWidgets.QHBoxLayout()
        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Filter by extension...")
        btn_reload = QtWidgets.QPushButton("Reload")
        btn_reload.clicked.connect(self.reload)
        self.combo_category = QtWidgets.QComboBox()
        self.combo_category.addItems([c.capitalize() for c in CATEGORIES if c != "unknown"])  # map to non-unknown
        btn_map = QtWidgets.QPushButton("Map Selected to Category & Save to Settings")
        btn_map.clicked.connect(self.map_selected)
        btn_reclass = QtWidgets.QPushButton("Reclassify Unknowns from Settings")
        btn_reclass.clicked.connect(self.reclassify_from_settings)
        toolbar.addWidget(self.search)
        toolbar.addWidget(btn_reload)
        toolbar.addWidget(self.combo_category)
        toolbar.addWidget(btn_map)
        toolbar.addWidget(btn_reclass)
        layout.addLayout(toolbar)

        # Table of unknown extensions
        self.table = QtWidgets.QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Extension", "Count", "Sample Path"])
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        self.reload()

    def _fetch_unknowns(self) -> List[Tuple[str, int, str]]:
        rows = dbm.list_unknown_extensions(self.conn)
        # rows: (ext, count, sample_path)
        return rows

    def reload(self):
        rows = self._fetch_unknowns()
        q = self.search.text().strip().lower()
        if q:
            rows = [r for r in rows if q in (r[0] or "").lower()]
        self.table.setRowCount(0)
        for ext, cnt, sample in rows:
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QtWidgets.QTableWidgetItem(str(ext or "")))
            self.table.setItem(r, 1, QtWidgets.QTableWidgetItem(str(cnt)))
            self.table.setItem(r, 2, QtWidgets.QTableWidgetItem(str(sample or "")))
        self.table.resizeColumnsToContents()

    def _selected_extensions(self) -> List[str]:
        exts: List[str] = []
        for idx in self.table.selectionModel().selectedRows():
            ext = self.table.item(idx.row(), 0).text().strip()
            if ext:
                exts.append(ext)
        return exts

    def map_selected(self):
        exts = self._selected_extensions()
        if not exts:
            return
        cat = self.combo_category.currentText().lower()
        # Confirm
        if QtWidgets.QMessageBox.question(self, "Map Unknowns", f"Map {len(exts)} extensions to '{cat}' and update settings?") != QtWidgets.QMessageBox.Yes:
            return
        # Update settings lists
        for e in exts:
            if cat == "video":
                if e not in [x.lower() for x in self.settings.media_extensions]:
                    self.settings.media_extensions.append(e)
            elif cat == "image":
                if e not in [x.lower() for x in getattr(self.settings, 'image_extensions', [])]:
                    self.settings.image_extensions.append(e)
            elif cat == "subtitle":
                if e not in [x.lower() for x in getattr(self.settings, 'subtitle_extensions', [])]:
                    self.settings.subtitle_extensions.append(e)
            elif cat == "xml":
                if e not in [x.lower() for x in getattr(self.settings, 'xml_extensions', [])]:
                    self.settings.xml_extensions.append(e)
            elif cat == "other":
                if e not in [x.lower() for x in getattr(self.settings, 'other_extensions', [])]:
                    self.settings.other_extensions.append(e)
        # Update DB categories for those extensions
        for e in exts:
            dbm.set_category_for_extension(self.conn, e, cat)
        self.reload()

    def reclassify_from_settings(self):
        # For each unknown extension, if it is in any list, set category accordingly
        rows = self._fetch_unknowns()
        changed = 0
        vid = set(x.lower() for x in self.settings.media_extensions)
        imgs = set(x.lower() for x in getattr(self.settings, 'image_extensions', []))
        subs = set(x.lower() for x in getattr(self.settings, 'subtitle_extensions', []))
        xmls = set(x.lower() for x in getattr(self.settings, 'xml_extensions', []))
        others = set(x.lower() for x in getattr(self.settings, 'other_extensions', []))
        for ext, cnt, sample in rows:
            e = (ext or '').lower()
            if e in vid:
                dbm.set_category_for_extension(self.conn, e, 'video'); changed += 1
            elif e in imgs:
                dbm.set_category_for_extension(self.conn, e, 'image'); changed += 1
            elif e in subs:
                dbm.set_category_for_extension(self.conn, e, 'subtitle'); changed += 1
            elif e in xmls:
                dbm.set_category_for_extension(self.conn, e, 'xml'); changed += 1
            elif e in others:
                dbm.set_category_for_extension(self.conn, e, 'other'); changed += 1
        if changed:
            QtWidgets.QMessageBox.information(self, "Reclassify", f"Updated {changed} extension mappings from settings.")
        self.reload()
