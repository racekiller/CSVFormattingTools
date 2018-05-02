from PyQt5 import QtGui, QtCore, QtWidgets
import sys, os
from utils import *
pd.options.mode.chained_assignment = None  # default='warn'

import mainwindow2

class MainWindow(QtWidgets.QMainWindow, mainwindow2.Ui_MainWindow):
    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        self.setupUi(self)
        self.BtnRAWFiles.clicked.connect(self.browse_folder_tobeprocessed)
        self.BtnPathPROCFiles.clicked.connect(self.browse_folder_processed)
        self.BtnReadCSV.clicked.connect(self.ReadCSVfile)

    def browse_folder_tobeprocessed(self):
        global ToBeProcessedFolder
        self.ToBeProcessedPath.clear()
        ToBeProcessedFolder = QtWidgets.QFileDialog.getExistingDirectory(self, "Pick a folder")
        self.ToBeProcessedPath.addItem(ToBeProcessedFolder)
        print(ToBeProcessedFolder)

    def browse_folder_processed(self):
        global ProcessedFolder
        self.ProcessedPath.clear()
        ProcessedFolder = QtWidgets.QFileDialog.getExistingDirectory(self, "Pick a folder")
        self.ProcessedPath.addItem(ProcessedFolder)
        print(ProcessedFolder)

    def ReadCSVfile(self):
        self.ListOfSensors.clear()
        self.ListOfTextFound.clear()

        ToBeProcessedFolder = "/Users/jvivas/Documents/Aspen/TJ/to be processed"
        CSVFileList = GetCSVList(ToBeProcessedFolder)
        CSVFile = CSVFileList[0]
        CSVFileWithPath = ToBeProcessedFolder + "/" + CSVFileList[0]
        csvFileSizeGB = GetFileSize(CSVFileWithPath)

        if csvFileSizeGB > 1:
            StringListForAllSensors = SplitCSVFile_GetStrings(CSVFileWithPath, CSVFile)
            print('These are all the strings found in the file: ' + str(StringListForAllSensors))
        else:

            df2 = LoadCSV(CSVFileWithPath, CSVFile)

            # Rename date and time column
            print('Renaming date and time column')
            RenameColumn(df2)
            # Apply Date and time format to dataframe
            print('Applying date and time format')
            ApplyDateFormat(df2)

            # Apply Index and create two dataframes
            df2_1, df2_2 = SetIndex(df2)

            print('Extracting strings from csv file')
            StringListForAllSensors, ListOfSensors = ExtractStrings(df2_1, df2_2)

            # populate list of sensor in GUI
            for sensor in ListOfSensors:
                self.ListOfSensors.addItem(sensor)

            # populate list of text per sensor in GUI
            for textInSensors in StringListForAllSensors:
                self.ListOfTextFound.addItem(textInSensors)

            print('These are all the strings found in the csv file: ' + str(StringListForAllSensors))

        # Convert all strings to a Dictionary
        StringListDict = {}.fromkeys(StringListForAllSensors, 'null')

        # print ("# Run following line to see the list of Strings")
        print(StringListDict)

        # Fill in table with Text and get input number from user

        # resize number of rows
        self.TextPerSensorTable.setRowCount(len(StringListForAllSensors))

        # fill table with text found per sensor
        for i in range(len(StringListForAllSensors)):
            #print(StringListForAllSensors[i])
            #print(str(i))
            self.TextPerSensorTable.setItem(0, i, QtWidgets.QTableWidgetItem(StringListForAllSensors[i]))

            #this is how we read from the table
            print(self.TextPerSensorTable.item(0,0).text())


def main():
    app = QtWidgets.QApplication(sys.argv)
    form = MainWindow()
    form.show()
    app.exec()

if __name__ == '__main__':
    main()
