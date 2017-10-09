
# coding: utf-8

# In[1]:

# latest update
# October 1st 2017
# Changes done by: Jimmy Vivas
# Added code description

# This code was developed by Jimmy Vivas

# The code will take all CSv files from a specific directory and process them at the same Time
# The program will need manual input to replace strings found for numeric data or null value
#   It will assign an integer value for each distinct string for each column (sensor)
# The code will generate as many csv files pero 1MM rows


# # STEP 1 -. Update the path where the CSV file is located and hit SHIFT + ENTER

#
# ### Create two folders
# #### Folder 1 = CSVPAath1 is where the CSV file to be processed is located
# #### Folder 2 = FinalPath is where all the files processed will be created

# In[2]:

# Windows Path
CSVPath1 = 'C:/Users/vivasj/Documents/Aspen/MOTIVA/Jacob Data/to be processed'
FinalPath = 'C:/Users/vivasj/Documents/Aspen/MOTIVA/Jacob Data/to be processed'


# # STEP 2-. Execute Next line Hitting Shift+ENTER

# In[8]:

from utils import *


# In[4]:

CSVFileList = GetCSVList(CSVPath1)

CSVFile = CSVFileList[0]
CSVFileWithPath = CSVPath1 + "/" + CSVFileList[0]

csvFileSizeGB = GetFileSize(CSVFileWithPath)

if csvFileSizeGB > 1:
    StringListForAllSensors = SplitCSVFile_GetStrings(CSVFileWithPath, CSVFile)
    print ('These are all the strings found in the file: ' + str(StringListForAllSensors))
else:

    df2 = LoadCSV(CSVFileWithPath, CSVFile)

    # Rename date and time column
    print ('Renaming date and time column')
    RenameColumn(df2)
    # Apply Date and time format to dataframe
    print ('Applying date and time format')
    ApplyDateFormat(df2)

    # Apply Index and create two dataframes
    df2_1, df2_2 = SetIndex(df2)

    print ('Extracting strings from csv file')
    StringListForAllSensors = ExtractStrings(df2_1, df2_2)

    print ('These are all the strings found in the csv file: ' + str(StringListForAllSensors))

# Convert all strings to a Dictionary
StringListDict = {}.fromkeys(StringListForAllSensors, 'null')

# print ("# Run following line to see the list of Strings")
## Copy the result and paste it in the following line of code")
print (StringListDict)


# # STEP 3 -. Copy the StringListDict and replace the null with the desired numeric data

# In[7]:

# Replacing known test to null
df2_2.replace(StringListDict, inplace=True, regex=True)

# Merge dataframes to export to CSV
mdf = FormatToPrevise(df2_1,df2_2)

# Check format data to be exportedaa
# print (mdf.head(25))

# Export to CSV
SplitPreviseFormatCSVFile(mdf,CSVFileList,FinalPath)

