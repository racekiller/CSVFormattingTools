# CSVFormattingTools
In here I will put any code to process data files (data wrangling, data monging)

## CSVFormattingTool_Format_Exxon
 The code will take all CSv files from a specific directory and process them at the same time
 It will take the UNIT from second row and concatenate it to the description
 remove the UNIT row
 Date and Time are in different columns, the code will merge them and apply timestamp format
 The code will get replace strings in the values for numeric data
   It will assign an integer value for each distinct string for each column (sensor)
   The integer value will be reset it for each sensor
   The code does not evaluare wether or not the sensor should stay. THe analyst will make this decision
   Using Previse or any other tool
 The code will split the csv file if the result is more than 1MM rows

 ## CSVFormattingTool
 The code will take all CSv files from a specific directory and process them at the same time
 The code will get replace strings in the values for numeric data
   It will assign an integer value for each distinct string for each column (sensor)
   The integer value will be reset it for each sensor
   The code does not evaluare wether or not the sensor should stay. THe analyst will make this decision
   Using Previse or any other tool
 The code will split the csv file if the result is more than 1MM rows

 ## XOMCSVFormattingTool_ExxonSYSTEM1_PHD_SensorUpdateName
 This code was developed by Jimmy Vivas
 The purpose of this code is to change the sensor name from SYSTEM1 to the PHD Historian.
 The code will take all CSv files from a specific directory
 each file must be one sensor
 each file should has the name of the sensor from SYSTEM1
 the code will fix the date and time and will add the description and update the sensor name as well.