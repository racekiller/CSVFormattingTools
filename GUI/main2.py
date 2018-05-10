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
        self.BtnGenerateMtellFiles.clicked.connect(self.CreateCSVFiles)

    def browse_folder_tobeprocessed(self):
        # global ToBeProcessedFolder
        self.ToBeProcessedPath.clear()
        self.ToBeProcessedFolder = QtWidgets.QFileDialog.getExistingDirectory(self, "Pick a folder")
        self.ToBeProcessedPath.addItem(self.ToBeProcessedFolder)
        print(self.ToBeProcessedFolder)

    def browse_folder_processed(self):
        # global ProcessedFolder
        self.ProcessedPath.clear()
        self.ProcessedFolder = QtWidgets.QFileDialog.getExistingDirectory(self, "Pick a folder")
        self.ProcessedPath.addItem(self.ProcessedFolder)
        print(self.ProcessedFolder)

    def ReadCSVfile(self):
        # global StringListDict, CSVFileList
        self.ListOfSensors.clear()
        self.ListOfTextFound.clear()

        # ToBeProcessedFolder = "/Users/jvivas/Documents/Aspen/TJ/to be processed"
        self.CSVFileList = GetCSVList(self.ToBeProcessedFolder)
        CSVFile = self.CSVFileList[0]
        CSVFileWithPath = self.ToBeProcessedFolder + "/" + self.CSVFileList[0]
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
        self.StringListDict = {}.fromkeys(StringListForAllSensors, 'null')

        # print ("# Run following line to see the list of Strings")
        print(self.StringListDict)

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
        for Text in self.StringListDict:
            self.StringListDict[Text] = self.TextPerSensorTable.item(i,1).text()
            i = i + 1

        # printing text vs number
        print(self.StringListDict)

        # Replacing text with number or null from user input
        self.df2_2 = ReplaceStrings(self.df2_2, self.StringListDict)

        # Create dataframe to export individual tags
        self.df_final = pd.concat([self.df2_1, self.df2_2])

    def CreateCSVFiles(self):
        # Export Individual Tags to CSV
        print("Creating CSV per TagName")
        ExportTagNamesToCSV(self.df_final, self.ProcessedFolder)

        # Merge dataframes to export to CSV
        mdf = FormatToPrevise(self.df2_1, self.df2_2)

        # Check format data to be exported
        # print(mdf.head(25))

        # Export to CSV
        print('Creating CSVs to be imported into Aspen Mtell')
        SplitPreviseFormatCSVFile(mdf, self.CSVFileList, self.ProcessedFolder)

        print("Done, check the processed folder")


def main():
    app = QtWidgets.QApplication(sys.argv)
    form = MainWindow()
    form.show()
    app.exec()

if __name__ == '__main__':
    main()
