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
        self.BtnReplaceText.clicked.connect(self.Replace_text)

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
        global StringListDict, CSVFileList
        self.ListOfSensors.clear()
        self.ListOfTextFound.clear()

        # ToBeProcessedFolder = "/Users/jvivas/Documents/Aspen/TJ/to be processed"
        CSVFileList = GetCSVList(ToBeProcessedFolder)
        CSVFile = CSVFileList[0]
        CSVFileWithPath = ToBeProcessedFolder + "/" + CSVFileList[0]
        csvFileSizeGB = GetFileSize(CSVFileWithPath)

        if csvFileSizeGB > 1:
            StringListForAllSensors = SplitCSVFile_GetStrings(CSVFileWithPath, CSVFile)
            print('These are all the strings found in the file: ' + str(StringListForAllSensors))
        else:

            self.df2 = LoadCSV(CSVFileWithPath, CSVFile)

            # Rename date and time column
            print('Renaming date and time column')
            RenameColumn(self.df2)
            # Apply Date and time format to dataframe
            print('Applying date and time format')
            ApplyDateFormat(self.df2)

            # Apply Index and create two dataframes
            self.df2_1, self.df2_2 = SetIndex(self.df2)

            print('Extracting strings from csv file')
            StringListForAllSensors, ListOfSensors = ExtractStrings(self.df2_1, self.df2_2)

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
            self.TextPerSensorTable.setItem(i, 0, QtWidgets.QTableWidgetItem(StringListForAllSensors[i]))

    def Replace_text(self):

        # update StringListDict with numbers input from user
        i = 0
        for Text in StringListDict:
            StringListDict[Text] = self.TextPerSensorTable.item(i,1).text()
            i = i + 1

        # printing text vs number
        print(StringListDict)

        # Replacing text with number or null from user input
        self.df2_2 = ReplaceStrings(self.df2_2, StringListDict)

        # Create dataframe to export individual tags
        self.df_final = pd.concat([self.df2_1, self.df2_2])

    def CreateCSVFiles(self):
        # Export Individual Tags to CSV
        print("Creating CSV per TagName")
        ExportTagNamesToCSV(self.df_final, ProcessedFolder)

        # Merge dataframes to export to CSV
        mdf = FormatToPrevise(self.df2_1, self.df2_2)

        # Check format data to be exported
        # print(mdf.head(25))

        # Export to CSV
        print('Creating CSVs to be imported into Aspen Mtell')
        SplitPreviseFormatCSVFile(mdf, CSVFileList, ProcessedFolder)

        print("Done, check the processed folder")


def main():
    app = QtWidgets.QApplication(sys.argv)
    form = MainWindow()
    form.show()
    app.exec()

if __name__ == '__main__':
    main()
