# In[1]:
from os import listdir
import pandas as pd
# In[2]:
CSVFileList = []
NanPHDTagList = []
# mac Path
# CSVPath1 = '/Users/jvivas/Dropbox/Mtell Customer Projects/' \
#             'XOM BayTown RHC MEA Tower Foaming/Sensor Data'

# Windows Path
CSVPath1 = 'C:/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BayTown RHC MEA Tower Foaming/Sensor Data/' \
           'To be Processed'

# CSVPath2 = '/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BMT Phase 2 Live Monitoring/Sensor Data/System 1 Data'

# In[3]:
CSVFileListAll = listdir(CSVPath1)
m = len(CSVFileListAll)
for i in range(m):
    fileNameStr = CSVFileListAll[i]
    fileStr = fileNameStr.split('.')[0]
    fileExt = fileNameStr.split('.')[1]
    if fileExt == "csv":
        CSVFileList.append(fileNameStr)

n = len(CSVFileList)
# In[4]:
for i in range(n):
    csvfile = CSVPath1 + "/" + CSVFileList[i]
    print("Loading Segment: %s" % CSVFileList[i])

    # In[43]:
    df = pd.read_csv(csvfile, index_col=False, sep=',', low_memory=False)

    # In[44]:
    # Concatenate DATE and TIME Column
    df['DATETIME2'] = df['DATE'] + ' ' + df['TIME']

    # Replace the "-" in case datetime is following format: "05-Jan-2015"
    df['DATETIME2'].replace({'-': ''}, inplace=True, regex=True)
    # In[51]:
    # Change new datetime column to datetime format
    df['DATETIME2'] = pd.to_datetime(df['DATETIME2'], format="%d%b%y %H:%M:%S")

    # In[53]:
    # Change datetime column format to look like 01/31/2015 0:00:00
    df['DATETIME2'] = df['DATETIME2'].dt.strftime('%m/%d/%Y %H:%M:%S')

    # In[59]:
    # Work around to move datetime column to be the first column in the dataset
    df['DATE'] = df['DATETIME2']

    # In[63]:
    # Rename DATE column to DATETIME
    df = df.rename(columns={'DATE': 'DATETIME'})

    # In[61]:
    # Delete unnecessary columns
    del df['TIME']
    del df['DATETIME2']

    # Code to do Transposing
    # Create two dataframes df1 only with tags and descriptions. df2 tag with values
    df1 = df[0:1]  # FIRST ROW
    df2 = df[1:len(df)]  # SECOND TO LAST ROW

    # Converting Historian files to VTQ format (DATETIME, TAGNAME, DESCRIPTION, VALUE)
    mdf = pd.merge(pd.melt(df1, id_vars=['DATETIME'], var_name='TAGNAME',
                           value_name='DESCRIPTION')[['TAGNAME', 'DESCRIPTION']],
                   pd.melt(df2, id_vars=['DATETIME'], var_name='TAGNAME',
                           value_name='VALUE'), on=['TAGNAME'])

    # Sort columns by VTQ format
    mdf = mdf[['DATETIME', 'TAGNAME', 'DESCRIPTION', 'VALUE']]

    print('Exporting ' + CSVFileList[i] + 'Historian File')
    print("")

    # Exporting PHD Tag CSV file
    mdf.to_csv(CSVPath1 + '/' + str(CSVFileList[i].replace('.csv', '')) + '_Formatted.csv', index=False)
