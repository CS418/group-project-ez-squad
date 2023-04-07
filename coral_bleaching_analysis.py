
# Author - Khizer Shareef
#          EZsquad 
# cite  -  https://www.nature.com/articles/s41597-022-01121-y
#       -  https://www.nature.com/articles/s41597-022-01121-y/figures/3
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3


def sql_get( dbCursor, *query_args ):
    # fetch the query from the database and store in row
    # if just query and no vars
    if len(query_args) == 1 :
        dbCursor.execute(query_args[0])
    else: # with query and vars
        dbCursor.execute(query_args[0], query_args[1:])
    row = dbCursor.fetchall();
    # return the [0][0] element of the query result - used when the query return is a singelton
    if (row != None) & ( len(row) == 1 ):
        if ( len(row[0]) == 1 ):
            return row[0][0]
    # else return the list of tuples of the result        
    return row


def print_stats(dbConn):
     # connection to db
    dbCursor = dbConn.cursor()
    hell = sql_get ( dbCursor, "SELECT COUNT(*) FROM Bleaching_tbl;")
    print("Total Rows:", hell)

    # get the reef check data from the db specially the S1,S2,S3,S4, ocean, country, year and data source
    reef_check = sql_get ( dbCursor, "SELECT Ocean_Name_LUT.Ocean_Name, Country_Name_LUT.Country_Name, Date_Year, (S1), (S2), (S3), (S4), Data_Source_LUT.Data_Source \
                                    FROM Data_Source_LUT\
                                    JOIN Site_Info_tbl ON Site_Info_tbl.Data_Source = Data_Source_LUT.Data_Source_ID\
                                    JOIN Ocean_Name_LUT ON Site_Info_tbl.Ocean_Name = Ocean_Name_LUT.Ocean_ID\
                                    JOIN Country_Name_LUT ON Site_Info_tbl.Country_Name = Country_Name_LUT.Country_ID\
                                    JOIN Sample_Event_tbl ON Sample_Event_tbl.Site_ID = Site_Info_tbl.Site_ID\
                                    JOIN Bleaching_tbl ON Bleaching_tbl.Sample_ID = Sample_Event_tbl.Sample_ID\
                                    WHERE Date_Year BETWEEN 1980 AND 2020\
                                    AND Country_Name_LUT.Country_Name IS NOT NULL\
                                    AND Bleaching_tbl.S1 IS NOT NULL AND Bleaching_tbl.S2 IS NOT NULL AND Bleaching_tbl.S3 IS NOT NULL AND Bleaching_tbl.S4 IS NOT NULL\
                                    AND Bleaching_tbl.Percent_Bleached IS NULL\
                                    AND Data_Source_LUT.Data_Source like 'Reef_Check';")

    # other data sources that are like Donner, etc.
    other_sources = sql_get ( dbCursor, "SELECT Ocean_Name_LUT.Ocean_Name, Country_Name_LUT.Country_Name, Date_Year, Data_Source_LUT.Data_Source, Bleaching_tbl.Percent_Bleached \
                                FROM Data_Source_LUT\
                                JOIN Site_Info_tbl ON Site_Info_tbl.Data_Source = Data_Source_LUT.Data_Source_ID\
                                JOIN Ocean_Name_LUT ON Site_Info_tbl.Ocean_Name = Ocean_Name_LUT.Ocean_ID\
                                JOIN Country_Name_LUT ON Site_Info_tbl.Country_Name = Country_Name_LUT.Country_ID\
                                JOIN Sample_Event_tbl ON Sample_Event_tbl.Site_ID = Site_Info_tbl.Site_ID\
                                JOIN Bleaching_tbl ON Bleaching_tbl.Sample_ID = Sample_Event_tbl.Sample_ID\
                                WHERE Date_Year BETWEEN 1980 AND 2020\
                                AND Country_Name_LUT.Country_Name IS NOT NULL\
                                AND Bleaching_tbl.S1 IS NULL AND Bleaching_tbl.S2 IS NULL AND Bleaching_tbl.S3 IS NULL AND Bleaching_tbl.S4 IS NULL\
                                AND Bleaching_tbl.Percent_Bleached IS NOT NULL\
                                AND Data_Source_LUT.Data_Source != 'Reef_Check';")
    # print(" Valid Data:", other_sources )
    return reef_check, other_sources


# sql connection to db and get data that is not reef check
dbConn = sqlite3.connect('Global_Coral_Bleaching.db')
reef_data, other_data = print_stats(dbConn)
# dbConn.close()

df = pd.DataFrame(reef_data, columns=['Ocean', 'Country', 'Year', 'S1', 'S2', 'S3', 'S4', 'Data_Source'])
print ("Reef Check:", df.shape)

# create a dataframe from the data
df2 = pd.DataFrame(other_data, columns=['Ocean', 'Country', 'Year', 'Data_Source', 'Percent_Bleached'])
print ("Other Sources:", df2.shape)

#  min and max years in df
min_year = df['Year'].min()
max_year = df['Year'].max()
print("Min Year:", min_year)
print("Max Year:", max_year)


# lets check if there are any null values and if so lets drop them
if df.isnull().values.any():
    print("Reef Check Null Values:", df.isnull().sum())
    df = df.dropna()
else:
    print("Reef Check Null Values: None")

# check if there are any null values for df2 then print how many rows of them are null
if df2.isnull().values.any():
    print("Other Sources Null Values:", df2.isnull().sum())
    df2 = df2.dropna()
else:
    print("Other Sources Null Values: None")    

# lets get the average of S1,S2,S3,S4 and add it to a new column Average_bleaching
df['Percent_Bleached'] = df[['S1', 'S2', 'S3', 'S4']].mean(axis=1)

# lets drop S1,S2,S3,S4 and Reef Name columns from df since we calculated the average of the bleaching levels 
df = df.drop(['S1', 'S2', 'S3', 'S4'], axis=1)

# convert the year column to int removing the decimal places
df['Year'] = df['Year'].astype(int)
df2['Year'] = df2['Year'].astype(int)

# round the percent bleached column to 2 decimal places in df2 and df
df['Percent_Bleached'] = df['Percent_Bleached'].round(2)
df2['Percent_Bleached'] = df2['Percent_Bleached'].round(2)

print(df.head())
print(df2.head())  

# let us combine the two dataframes into one dataframe using concat
frames = [df, df2]
df = pd.concat(frames)
# lets sort the year column ascending order
df = df.sort_values(by=['Year'], ascending=True)
print(df.head())
print(df.shape)

#  min and max years in df
min_year = df['Year'].min()
max_year = df['Year'].max()
print("Min Year:", min_year)
print("Max Year:", max_year)


# group data by year and ocean and calculate the mean bleaching level
grouped = df.groupby(['Year', 'Ocean'])['Percent_Bleached'].mean()

# create a pivot table with years as the index and oceans as the columns
pt = pd.pivot_table(grouped.reset_index(), values='Percent_Bleached', index='Year', columns='Ocean')

# plot the pivot table
ax = pt.plot(kind='line', figsize=(10, 6), marker='o')

# set plot labels and title
ax.set_xlabel('Year')
ax.set_ylabel('Mean Bleaching Level')
ax.set_title('Yearly Increase in Coral Bleaching Level by Ocean')
plt.show()