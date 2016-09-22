
# coding: utf-8

# In[1]:

from os import listdir


# In[2]:

import pandas as pd


# In[3]:

CSVFileList = []
NanPHDTagList = []
CSVPath1 = '/Users/jvivas/Dropbox/Mtell Customer Projects/'             'XOM BayTown RHC MEA Tower Foaming/Sensor Data'


# In[4]:

CSVFileListAll = listdir(CSVPath1)
m = len(CSVFileListAll)
for i in range(m):
    fileNameStr = CSVFileListAll[i]
    fileStr = fileNameStr.split('.')[0]
    fileExt = fileNameStr.split('.')[1]
    if fileExt == "csv":
        CSVFileList.append(fileNameStr)


# In[6]:

print (CSVFileList)


# In[7]:

n = len(CSVFileList)


# In[8]:

for i in range(n):
    csvfile = CSVPath1 + "/" + CSVFileList[i]
    print("Loading Segment: %s" % CSVFileList[i])


# In[20]:

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


# In[65]:

del df['datetime'] 


# In[63]:

df=df.rename(columns = {'DATE':'DATETIME'})


# In[64]:

df


# In[22]:

df.to_csv(CSVPath1 + '/' + str(CSVFileList[i]) + '_' + 'Test' + '.csv', index=False)

