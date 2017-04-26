# This code was developed by Jimmy Vivas
# The purpose of this code is to change the sensor name from SYSTEM1 to the PHD Historian.
# The code will take all CSv files from a specific directory
# each file must be one sensor
# each file should has the name of the sensor from SYSTEM1
# the code will fix the date and time and will add the description and update the sensor name as well.

# latest update
# April 26, 2017
# Changes done by: Jimmy Vivas
# Added comments to the code

from os import listdir
import pandas as pd

SegmentList = []
NanPHDTagList = []

# Mac Path
CSVPath1 = '/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BMT Phase 2 ' \
           'Live Monitoring/Sensor Data/System 1 Data/Wave ' \
           '2/Daniel/System 1 - Phase 2 Pump P8232/2015 to 2016'
CSVPath2 = '/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BMT Phase 2 Live Monitoring/Sensor Data/System 1 Data'

# Windows Path
# CSVPath1 = 'C:/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BMT Phase 2 Live Monitoring/Sensor Data/System 1 Data' \
#            '/Received/Phase 1 - 2015-2016/Recip Comp/Second Part/C-457'
# CSVPath2 = 'C:/Users/jvivas/Dropbox/Mtell Customer Projects/XOM BMT Phase 2 Live Monitoring/Sensor Data/System 1 Data'


SegmentFileList = listdir(CSVPath1)
m = len(SegmentFileList)
for i in range(m):
    fileNameStr = SegmentFileList[i]
    fileStr = fileNameStr.split('.')[0]
    fileExt = fileNameStr.split('.')[1]
    if fileExt == "csv":
        SegmentList.append(fileStr)
# print(SegmentList)
# print ('the classifier came back with: %d, the real answer is: %d'
#             % (classifierResult, classNumStr))

# Import SYSTEM1 vs PHD Tag Cross Reference

CrossReferenceFile = 'SYSTEM1 vs PHD Tags Rev Daniel XOM.csv'
CrossReference = CSVPath2 + "/" + CrossReferenceFile
print('Loading SYSTEM1 Vs PHD Tag CrossReference csv file')
dfSYS1vsPHDTags = pd.read_csv(CrossReference, sep=',')

n = len(SegmentList)

for i in range(n):
    csvfile = CSVPath1 + "/" + SegmentList[i] + '.csv'
    print("Loading Segment: %s" % SegmentList[i])

    try:
        # Get PHD Tag
        PHDTag = dfSYS1vsPHDTags.loc[dfSYS1vsPHDTags['Segment ID'] == int(SegmentList[i]), 'Converted PHD Tag'].values[
            0]
        print(PHDTag)

        # Get SYSTEM1 Tag Description
        PHDTagDescription = \
        dfSYS1vsPHDTags.loc[dfSYS1vsPHDTags['Segment ID'] == int(SegmentList[i]), 'System 1 Description'].values[0]
    except IndexError:
        # print ('The error: %s' % e)
        continue

    if isinstance(PHDTag, str) == False:
        # math.isnan(float(PHDTag)):
        NanPHDTagList.append(PHDTag)
        continue

    df = pd.read_csv(csvfile, sep=',', names=["TimeStamp", "TBD", "TBD", "Value", "TBD"], low_memory=False)

    # delete TBD columns
    del df['TBD']

    # Remove last 4 characters from the date string
    df['TimeStamp'] = df['TimeStamp'].str[:-4]

    # Convert string date to date format mm/dd/yyyy hh:mm:ss
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp']).dt.strftime('%m/%d/%Y %H:%M:%S')

    # Add Tagname and description to dataframe
    df['TagName'] = PHDTag
    df['Description'] = PHDTagDescription

    print('Exporting PHD Tag CSV file')
    print("")

    # Exporting PHD Tag CSV file
    df.to_csv(CSVPath1 + '/' + str(SegmentList[i]) + '_' + PHDTag + '.csv', index=False)

dfNanPHDTagList = pd.DataFrame(NanPHDTagList)

print('Exporting PHD Tag Nulls CSV file')
dfNanPHDTagList.to_csv(CSVPath1 + '/' + 'PHDTagNulls.csv', index=False)
