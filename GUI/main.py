from PyQt5 import QtGui, QtCore, QtWidgets
import sys, os

import design

class ExampleApp(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def __init__(self, parent=None):
        super(ExampleApp, self).__init__(parent)
        self.setupUi(self)
        self.btnBrowse.clicked.connect(self.browse_folder)

    def browse_folder(self):
        self.listWidget.clear()
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Pick a folder")
        if directory:
            for file_name in os.listdir(directory):
                self.listWidget.addItem(file_name)

def main():
    app = QtWidgets.QApplication(sys.argv)
    form = ExampleApp()
    form.show()
    app.exec()

if __name__ == '__main__':
    main()