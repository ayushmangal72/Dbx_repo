Readme File

Step 1: 

* Imported all the necessary libraries and functions required for the task.

Step 2: 

* Loaded the given dataset in PySpark dataframe and typecasted the data type of columns as per the requirement using a dictionary.

Step 3: 

* Created a Timestamp column with the concatenation of the Date and Time columns in timestamp format. Then, dropped the Date and Time columns from the data frame. 

Step 4: 

* Filtered the dataframe with a magnitude column value greater than 5. After that, I calculated the average depth and magnitude for each earthquake type using this dataframe. 

Step 5: 

* Developed a UDF to categorize the earthquakes into different levels such as High, Moderate, and Low, based on their magnitude column value. 

Step 6: 

* Calculated the distance of each earthquake from the reference location (0,0). 

Step 7: 

* Visualized the geographical distribution of earthquakes on the world map using longitude, latitude, and magnitude columns. 

Step 8: 

* Saved the final output after the transformations on the data frame at a different location in CSV format.


Loom Video Link - https://drive.google.com/file/d/1Y--mrdV05r0DXdI2uG6_j1Hs9a3akLD4/view?usp=sharing
