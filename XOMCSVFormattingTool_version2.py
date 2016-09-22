# In[1]:
from os import listdir
import pandas as pd
# In[2]:
CSVFileList = []
NanPHDTagList = []
CSVPath1 = '/Users/jvivas/Dropbox/Mtell Customer Projects/' \
            'XOM BayTown RHC MEA Tower Foaming/Sensor Data'
#CSVPath2 = '/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BMT Phase 2 Live Monitoring/Sensor Data/System 1 Data'
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

    df = pd.read_csv(csvfile, index_col=False,sep=',', low_memory=False)
    
    # In[44]:
    
    df['datetime'] = df['DATE'] + ' ' + df['TIME']
    
    
    # In[51]:
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    
    # In[53]:
    
    df['datetime'] = df.datetime.dt.strftime('%m/%d/%Y %H:%M:%S')
    
    
    # In[59]:
    
    df['DATE'] = df['datetime'] 
    
    
    # In[61]:
    
    del df['TIME'] 
    del df['datetime']     
    
    # In[63]:
    
    df=df.rename(columns = {'DATE':'DATETIME'})

    print('Exporting PHD Tag CSV file')
    print("")

    # Exporting PHD Tag CSV file
    df.to_csv(CSVPath1 + '/' + str(CSVFileList[i]) + '_' + 'Formatted' + '.csv', index=False)