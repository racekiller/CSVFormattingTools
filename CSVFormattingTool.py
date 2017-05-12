
# coding: utf-8

# In[10]:

# latest update
# April 26, 2017
# Changes done by: Jimmy Vivas
# Added code description

# This code was developed by Jimmy Vivas

# The code will take all CSv files from a specific directory and process them at the same time
# The code will get replace strings in the values for numeric data
#   It will assign an integer value for each distinct string for each column (sensor)
#   The integer value will be reset it for each sensor
#   The code does not evaluare wether or not the sensor should stay. THe analyst will make this decision
#   Using Previse or any other tool
# The code will split the csv file if the result is more than 1MM rows


# In[11]:

from os import listdir
import pandas as pd
import math


# In[12]:

CSVFileList = []
NanPHDTagList = []


# In[13]:

# mac Path
#CSVPath1 =  '/Users/jvivas/Documents/XMT Baytwon Sensor Data' \
#            '/Sensor Data/Testing'

CSVPath1 = '/Users/jvivas/Documents/Aspen/PTT GC PoC/Sensor Data/A210 - Quench Oil Tower/To Be Processed'

# Windows Path
# CSVPath1 = 'C:/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BayTown RHC MEA Tower Foaming/Sensor Data/' \
#           'To be Processed'

# CSVPath1 = 'C:/Mtell/Projects/XOM Baytown POC/Sensor Data/ToBeFormatted'


# In[14]:

CSVFileListAll = listdir(CSVPath1)
m = len(CSVFileListAll)
for i in range(m):
    fileNameStr = CSVFileListAll[i]
    fileStr = fileNameStr.split('.')[0]
    fileExt = fileNameStr.split('.')[1]
    if fileExt == "csv":
        CSVFileList.append(fileNameStr)


# In[15]:

n = len(CSVFileList)


# In[16]:

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


# In[17]:

get_ipython().magic('debug')
i = 0
for i in range(n):
    csvfile = CSVPath1 + "/" + CSVFileList[i]
    print("Loading Segment: %s" % CSVFileList[i])

    # In[43]:
    df = pd.read_csv(csvfile, index_col=False, sep=',', low_memory=False)
    
    # In[51]:
    # Need to do this to avoid 
    df = df.copy()
    # Change new datetime column to datetime format
    df['Date TIme'] = pd.to_datetime(df['Date TIme'])
    # In[53]:
    # Change datetime column format to look like 01/31/2015 0:00:00
    df['Date TIme'] = df['Date TIme'].dt.strftime('%m/%d/%Y %H:%M:%S')

    # Code to do Transposing
    # Create two dataframes df1 only with tags and descriptions. df2 tag with values
    df1 = df[0:1]  # FIRST ROW
    df2 = df[1:len(df)]  # SECOND TO LAST ROW
    
    # Replacing known test to null
    df2.replace({'Bad': 'null', 'No Data': 'null', 'error': 'null'}, inplace=True, regex=True)

    # Convert columns to numbers those that has string wont be converted
    df2 = df2.apply(lambda x: pd.to_numeric(x, errors='ignore'))

    # Here we filter the columns that are objects and create a dataframe with those columns
    cols = df2.filter(like='I4').select_dtypes(include=['object']).columns

    # We need to do a for loop to go thru each column and convert  the characters string to numbers
    y = 0
    for y in range(len(cols)):
        # We don't want to change null to number
        if pd.unique(df2[[cols[1]]].values.ravel()) != 'null':
        # Here we are assign a integer to each unique row value, the integer will increase
        # from 0 to x where x is the total unique values
            df2[[df2.columns.get_loc(cols[y])]] = pd.factorize(df2.iloc[:,df2.columns.get_loc(cols[y])])[0]

    # Converting Historian files to VTQ format (DATETIME, TAGNAME, DESCRIPTION, VALUE)
    mdf = pd.merge(pd.melt(df1, id_vars=['Date TIme'], var_name='TAGNAME',
                           value_name='DESCRIPTION')[['TAGNAME']],
                   pd.melt(df2, id_vars=['Date TIme'],  var_name='TAGNAME',
                           value_name='VALUE'), on=['TAGNAME'])

    # Sort columns by VTQ format
    mdf = mdf[['Date TIme', 'TAGNAME', 'VALUE']]

    # Exporting PHD Tag CSV file
    j = 1
    rows = 1000000
    totalRows = len(mdf)
    loops = math.ceil(totalRows/rows) + 1

    for j in range(loops): #need to round this
        a = (rows*j) - rows
        if totalRows <= rows:
            b = totalRows
            print('Exporting ' + str(CSVFileList[i].replace('.csv', '')) + ' Historian File')
            print("")
            mdf[a:b].to_csv(CSVPath1 + '/' + str(CSVFileList[i].replace('.csv', '')) + '_Formatted.csv', index=False)
        else:
            if (rows*j) >= totalRows:
                b = totalRows
                print('Exporting ' + str(CSVFileList[i].replace('.csv', '')) + ' chunk' + str(j) + ' Historian File')
                print("")
                mdf[a:b].to_csv(CSVPath1 + '/' + str(CSVFileList[i].replace('.csv', '')) + '_Formatted_chunk' + str(j) + '.csv', index=False)
            else:
                b = (rows*j) - 1
                print('Exporting ' + str(CSVFileList[i].replace('.csv', '')) + ' chunk' + str(j) + ' Historian File')
                print("")
                mdf[a:b].to_csv(CSVPath1 + '/' + str(CSVFileList[i].replace('.csv', '')) + '_Formatted_chunk' + str(j) + '.csv', index=False)


# In[ ]:



