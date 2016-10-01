# In[1]:
from os import listdir
import pandas as pd
import math
# In[2]:
CSVFileList = []
NanPHDTagList = []
# mac Path
# CSVPath1 = '/Users/jvivas/Dropbox/Mtell Customer Projects/' \
#             'XOM BayTown RHC MEA Tower Foaming/Sensor Data'

# Windows Path
#CSVPath1 = 'C:/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BayTown RHC MEA Tower Foaming/Sensor Data/' \
#           'To be Processed'
           
CSVPath1 = 'C:/Mtell/Projects/XOM Baytown POC/Sesor Data/Testing'

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
i = 0
for i in range(n):
    csvfile = CSVPath1 + "/" + CSVFileList[i]
    print("Loading Segment: %s" % CSVFileList[i])

    # In[43]:
    df = pd.read_csv(csvfile, index_col=False, sep=',', low_memory=False)

    # In[100]:
    # Here we add [UNIT] to the description
    x = 2   
    for x in range(len(df.columns)):
        if (df.iloc[1][x] != ' ') and (df.iloc[1][x] != '-') and (df.iloc[1][x] != 'NONE'):
            df.iloc[0][x] = df.iloc[0][x] + ' [' + df.iloc[1][x] + ']'
    
    # Here we remove the row #1 which is the UNIT row    
    df = df.drop(df.index[[1]])
                
    # In[44]:
    # Concatenate DATE and TIME Column
    df['DATETIME2'] = df['DATE'] + ' ' + df['TIME']

    # Replace the "-" in case datetime is following format: "05-Jan-2015"
    df['DATETIME2'].replace({'-': ''}, inplace=True, regex=True)
    # In[51]:
    # Change new datetime column to datetime format
    # df['DATETIME2'] = pd.to_datetime(df['DATETIME2'], format="%d%b%y %H:%M:%S")
    df['DATETIME2'] = pd.to_datetime(df.iloc[2:len(df),len(df.columns)-1], format="%d%b%y %H:%M:%S")
    
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

    # Convert columns to numbers those that has string wont be converted    
    df2 = df2.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    
    # We need to do a for loop to go thru each column and convert  the characters string to numbers
    
    # Here we need to go thru the columns that are objects only
    cols = df2.filter(like='FNR').select_dtypes(include=['object']).columns
    
    # Here we need to go thru the columns that are objects only
    y = 0    
    for y in range(len(cols)):
        # Here we are assign a integer to each unique row value, the integer will increase
        # from 0 to x where x is the total unique values
        df2[[df2.columns.get_loc(cols[y])]] = pd.factorize(df2.iloc[:,df2.columns.get_loc(cols[y])])[0]
    
    # Converting Historian files to VTQ format (DATETIME, TAGNAME, DESCRIPTION, VALUE)
    mdf = pd.merge(pd.melt(df1, id_vars=['DATETIME'], var_name='TAGNAME',
                           value_name='DESCRIPTION')[['TAGNAME', 'DESCRIPTION']],
                   pd.melt(df2, id_vars=['DATETIME'],  var_name='TAGNAME',
                           value_name='VALUE'), on=['TAGNAME'])

    # Sort columns by VTQ format
    mdf = mdf[['DATETIME', 'TAGNAME', 'DESCRIPTION', 'VALUE']]

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