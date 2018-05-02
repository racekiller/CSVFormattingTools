
# coding: utf-8

# In[1]:

# latest update
# October 1st 2017
# Changes done by: Jimmy Vivas
# Added code description

# This code was developed by Jimmy Vivas

# Find below all functions used in DataWranglingTool

from os import listdir
import pandas as pd
import math
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
# %matplotlib inline
# In[0]:

def GetCSVList(CSVPath1):
	from os import listdir
	CSVFileList = []
	NanPHDTagList = []
	CSVFileListAll = listdir(CSVPath1)

	m = len(CSVFileListAll)

	for i in range(m):
	    fileNameStr = CSVFileListAll[i]
	    fileStr = fileNameStr.split('.')[0]
	    fileExt = fileNameStr.split('.')[1]
	    if fileExt == "csv":
	        CSVFileList.append(fileNameStr)

	n = len(CSVFileList)
	return (CSVFileList)

# In[1]:

def GetFileSize(file):
    import os
    csvFileStatInfo = os.stat(file)
    csvFileSizeGB = csvFileStatInfo.st_size/1000000000
    return(csvFileSizeGB)


# In[2]:

def RenameColumn(df2):
# Ranane Date and time column to DATETIME
    new_cols = ['DATETIME']
    df2.rename(columns=dict(zip(df2.columns[[0]], new_cols)),inplace=True)
    df2.iloc[0,1] = ""
    return


# In[3]:

def ApplyDateFormat(df2):

    # duplicate df2 to apply date and time format
    df2 = df2.copy()
    # Change new datetime column to datetime format
    df2['DATETIME'] = pd.to_datetime(df2['DATETIME'])
    # Change datetime column format to look like 01/31/2015 0:00:00
    df2['DATETIME'] = df2['DATETIME'].dt.strftime('%m/%d/%Y %H:%M:%S')
    return


# In[4]:

def SplitCSVFile_GetStrings(CSVFileWithPath, CSVFile):
    chunksize = 25000

    i = 0
    j = 1

    SensorStringListAllChunks = []
    StringListForAllSensors = []
    StringListForEachChunk = []

    print ('Loading ' + CSVFile + ' file')
    for df in pd.read_csv(CSVFileWithPath, chunksize=chunksize, iterator=True, low_memory=False):
        # df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
        df.index += j
        i+=1
        j = df.index[-1] + 1
        h = 0

        # Rename date and time column
        print ('Renaming date and time column')
        RenameColumn(df)

        # Apply Date and time format to dataframe
        print ('Applying date and time format')
        ApplyDateFormat(df)

        # Code to do Transposing
        # Create two dataframes df1 only with tags and descriptions. df2 tag with values
        if i == 1:
            df1 = df[0:1]  # FIRST ROW
            # Indexing dataframe df1
            df1 = df1.set_index('DATETIME')
        else:
            if i > 1:
                df1 = df1

        df2 = df[1:len(df)]  # SECOND TO LAST ROW

        # Indexing dataframe df2
        df2 = df2.set_index('DATETIME')

        # Export CSV chunk for each loop
        print ('Exporting chunk' + str(i))
        df.to_csv(CSVFileWithPath.replace('.csv', '') + '_chunk_ ' + str(i) + '.csv', index=True)

        print ('Getting the list for chunk' + str(i))
        StringListForEachChunk = ExtractStrings(df1, df2)

        print ('These are all the strings found in the chunk ' + str(i) + ':' + str(StringListForEachChunk))

        SensorStringListAllChunks.extend(StringListForEachChunk)

    SensorStringListAllChunks = list(set(SensorStringListAllChunks))
    return(SensorStringListAllChunks)


# In[5]:

def ExtractStrings(df2_1, df2_2):

    print ('Converting df to numeric')
    # list of sensors
    ListOfSensors = df2_1.columns.tolist()
    print("List of Sensors")
    print(ListOfSensors)

    # Convert columns to numbers those that has string wil be converted to numpy null = NaN
    df2_with_nan = df2_2.apply(lambda x: pd.to_numeric(x, errors='coerce'))
    # print (df2_2.head())
    # print (df2_with_nan.head())
    # Ge the list of sensors with Strings
    df3 = pd.DataFrame(df2_with_nan.isnull().any(axis=0))
    df3 = df3.reset_index()
    SensorsWithStrings = df3[df3[0]==True]['index'].tolist()

    # Replace nan with 'null' and create new df
    df2_with_null = df2_with_nan.fillna(value='null')

    # Here we filter the columns that are objects and create a dataframe with those columns
    colsObject = df2_2.select_dtypes(include=['object']).columns
    TotalColumns = df2_2.columns

    print ('Total columns with text: ' + str(TotalColumns))
    print ('Sensors with text in their Values: ' + str(SensorsWithStrings))

    AllSensorsStringList = []

    if len(SensorsWithStrings) == 0:
        print ('No strings in this dataframe')
    else:
        # print (str(len(SensorsWithStrings)) + ' columns contain strings out of ' + str(len(TotalColumns)) + ' columns')
        # if (len(SensorsWithStrings)/len(TotalColumns)) > 0.5:
            # print ('this process will take some time depending of the size of the file and the PC resources')

        # Dropping DATETIME index to merge df1 and df2
        df2_1 = df2_1.reset_index(drop=False)
        df2_2 = df2_2.reset_index(drop=False)

        # Variable initialization
        j = 0

        StringListForCurrentSensor = []

        # Create dictionary from dataframe columns (sensors) that have strings only
        SensorDictionary = {}.fromkeys(SensorsWithStrings, [])

        # Loop to go thru each column and convert the characters string to numbers
        for j in range(len(SensorsWithStrings)):
            # from IPython.core.debugger import Tracer
            # Tracer()() #this one triggers the debugger

            # iterate thru each column in the dataframe
            # for j in range(len(list(SensorDictionary))):
            # update sensor name for each column
            Sensor = list(SensorDictionary)[j]

            print ('Processing Sensor: ' + Sensor)
            # Clear List of String for each Sensor
            SensorStringResult = []

            # Get rows that have null in the actual sensor column
            result = df2_with_nan[df2_with_nan[Sensor].isnull()][Sensor]

            # Convert list of nulls to dataframe and reset the datetime index
            result_df = pd.DataFrame(result)
            result_df = result_df.reset_index()

            # print ('printing result_df')
            # print (result_df.head())
            # print ('printing df2_2')
            # print (df2_2.head())
            # debugging
            # print (result_df)
            # debugging

            # Filtering rows with String for each sensor
            SensorStringResult = df2_2[df2_2['DATETIME'].isin(result_df.loc[:,"DATETIME"].values.tolist())][Sensor]

            # Adding Strings to Sensor in Sensor Dictionary
            # I think that when due to a bug the dictionary become too big it stops growing...
            # SensorDictionary[Sensor] = list(set(SensorStringResult))
            StringListForCurrentSensor = list(set(SensorStringResult))

            # Debugging
            # print (list(set(SensorStringResult)))
            # Debugging

            # print ('Sensor: ' + Sensor + ' Processed')

            # Adding String for Sensor to General String List
            AllSensorsStringList.extend(StringListForCurrentSensor)

            # Removing Duplicate Strings
            AllSensorsStringList = list(set(AllSensorsStringList))

			#**
			# The next lines will be disabled since I dont remember what they were for
            # result = {}
            # for key,value in StringListDict.items():
                # if value not in result.values():
                    # result[key] = value

            # print (result)
            print ('These are all the strings found in Sensor ' + Sensor + ': ' + str(StringListForCurrentSensor))
    return (AllSensorsStringList,ListOfSensors)


# In[6]:

def SetIndex(df2):
        # Code to do Transposing
    # Create two dataframes df1 only with tags and descriptions. df2 tag with values
    df2_1 = df2[0:1]  # FIRST ROW
    df2_2 = df2[1:len(df2)]  # SECOND TO LAST ROW

    # Indexing dataframe df1
    df2_1 = df2_1.set_index('DATETIME')

    # Indexing dataframe df2
    df2_2 = df2_2.set_index('DATETIME')
    return(df2_1, df2_2)


# In[7]:

def LoadCSV(FileAndPath, file):
    print ('Loading ' + file)
    df2 = pd.read_csv(FileAndPath, low_memory=False)
    return(df2)


# In[8]:

def FormatToPrevise(df2_1, df2_2):
    # Dropping DATETime index to merge df1 and df2
    df2_1 = df2_1.reset_index(drop=False)
    df2_2 = df2_2.reset_index(drop=False)

    # Converting Historian files to VTQ format (DATETime, TAGNAME, DESCRIPTION, VALUE)
    mdf = pd.merge(pd.melt(df2_1, id_vars=['DATETIME'], var_name='TAGNAME',
                           value_name='DESCRIPTION')[['TAGNAME', 'DESCRIPTION']],
                   pd.melt(df2_2, id_vars=['DATETIME'], var_name='TAGNAME',
                           value_name='VALUE'),
                   on=['TAGNAME'])

    # Sort columns by VTQ format
    mdf = mdf[['DATETIME', 'TAGNAME', 'DESCRIPTION', 'VALUE']]

    return (mdf)


# In[9]:

def SplitPreviseFormatCSVFile(mdf,CSVFileList,FinalPath):
    # Exporting PHD Tag CSV file
    i = 0
    rows = 1000000
    totalRows = len(mdf)
    loops = math.ceil(totalRows/rows)

    if totalRows > 1000000:
        for j in range(loops): #need to round this
            j = j + 1
            print (str(j))
            a = (rows*j) - rows
            if totalRows <= rows:
                b = totalRows
                print('Exporting ' + str(CSVFileList[i].replace('.csv', '')) +
                      ' Historian File')
                print("")
                mdf[a:b].to_csv(FinalPath + '/' + str(CSVFileList[i].replace(
                                '.csv', '')) + '_Formatted.csv', index=False)
            else:
                if (rows*j) >= totalRows:
                    b = totalRows
                    print('Exporting '
                          + str(CSVFileList[i].replace('.csv', ''))
                          + ' chunk' + str(j)
                          + ' Historian File')
                    print("")
                    mdf[a:b].to_csv(FinalPath + '/'
                       + str(CSVFileList[i].replace('.csv', ''))
                       + '_Formatted_chunk' + str(j)
                       + '.csv', index=False)
                else:
                    b = (rows*j) - 1
                    print('Exporting '
                          + str(CSVFileList[i].replace('.csv', ''))
                          + ' chunk' + str(j) + ' Historian File')
                    print("")
                    mdf[a:b].to_csv(FinalPath + '/'
                       + str(CSVFileList[i].replace('.csv', ''))
                       + '_Formatted_chunk' + str(j)
                       + '.csv', index=False)
    else:
        mdf.to_csv(FinalPath + '/'
                   + str(CSVFileList[0].replace('.csv', ''))
                   + '_Formatted' + '.csv', index=False)
    return

# In[10]:

def ReplaceStrings(df2_2,StringListDict):
    df2_2.replace(StringListDict, inplace=True, regex=True)
    return(df2_2)

def ExportTagNamesToCSV (df_concat, FinalPath):
    for j in range(len(df_concat.columns)):

        # Getting the tagname
        TagName = df_concat.columns[j]

        # Getting the tag Description
        TagDescriptionArray = df_concat.iloc[[0]][TagName].values
        TagDescription = TagDescriptionArray[0]

        df = df_concat.iloc[:,[j]]

        # Drop description from dataframe to be added as a column
        df.drop(df.index[0], inplace=True)
        df['TAGNAME'] = TagName
        df['TAGDESCRIPTION'] = TagDescription

        df = df.rename(columns = {TagName:'TAGVALUE'})

        df.to_csv(FinalPath + '/' + df_concat.columns[j]
                        + '.csv', index=True)

def GetLowStdDevSensors(df, path, StringDict):

    StringListDict_NaN = {'null': np.NaN}
    # Replacing null with numpy null
    df_NaN = ReplaceStrings(df, StringDict)

    # df['Ticks'] = range(0,len(df.index.values))
    StdDevAll = df_NaN.std(ddof=0).sort_values()
    StdDevAll_Below_1 = StdDevAll[StdDevAll<1]
    print('These are the sensors and StdDev for each below 1')
    print(StdDevAll_Below_1)
    StdDevAll_Below_1_df = pd.DataFrame(StdDevAll_Below_1).reset_index()
    StdDevAll_Below_1_df.columns = ['Sensors', 'Values']
    StdDevAll_Below_1_df = StdDevAll_Below_1_df.set_index('Sensors').T.columns

    print('Proceeding to export sensors with StdDev below 1 to PNG image')
    for i in range(len(StdDevAll_Below_1_df)):
        plt.plot_date(x=df_NaN['DATETIME'], y=df_NaN[StdDevAll_Below_1_df[i]], fmt="r-")
        plt.xlabel('DATETIME')
        plt.ylabel(StdDevAll_Below_1_df[i])
        plt.title('Instrument ' + df_NaN.columns[i] + ' Process Data')
        plt.savefig(path + StdDevAll_Below_1_df[i] +'.png', transparent = False, bbox_inches = 'tight', dpi = 300)
        plt.show()
    return (StdDevAll_Below_1_df)