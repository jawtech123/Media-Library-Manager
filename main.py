from __future__ import annotations
import sys
from PySide6 import QtWidgets

from app.settings import Settings
from app.gui.main_window import MainWindow


def main():
    app = QtWidgets.QApplication(sys.argv)
    settings = Settings()
    w = MainWindow(settings)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
