
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

    # findings from the data

    # Kumagai data has severity code 0,1,2,3
    #   (-1, '% unknown', 'T'), (0, 'No Bleaching', 'T'), (1, 'Mild (1-10% Bleached)', 'T'), (2, 'Moderate (11-50% Bleached)', 'T'), (3, 'Severe (>50% Bleached)', 'T') from severity_code_LUT
    # Donner data has severity code -1,0,1,2,3 and percent_bleached (some of them are None)
    #   (-1, '% unknown', 'T'), (0, 'No Bleaching', 'T'), (1, 'Mild (1-10% Bleached)', 'T'), (2, 'Moderate (11-50% Bleached)', 'T'), (3, 'Severe (>50% Bleached)', 'T') from severity_code_LUT

    # Reef_Check data has S1,S2,S3,S4, we can find averages of them 

    # AGRRA data has percent_bleached

    # FRRP data has percent_bleached - Atlantic Ocean data
    # .csv has Bleaching levels
    #   P = Pale (Tissue color is lighter than normal)
    #   PB = Partially Bleached (Portions of the coral have a complete loss of color)
    #   (BL) = Bleached (100% of coral tissue has lost its color and appears white)

    # McClanahan data has percent_bleached, Number__Bleached_Colonies, bleach_intensity 

    # Safaie data has Bleaching_Prevalence_Score_LUT
    #       ([(0, 'No Bleaching', 'T'), (1, '<= 10% Reef Area Bleached', 'T'), (2, '10-25% Reef Area Bleached', 'T'), (3, '25-50% Reef Area Bleached', 'T'), (4, '>50% Reef Area Bleached', 'T')])

    # all the data can be tied by 
    #   *donner needs to drop -1 values
    #   0 - no bleaching
    #   1 - mild 1-10%
    #   2 - moderate 11-50%
    #   3 - severe >50%

    #  Reef_check should be averaged and classified as above
    #  *AGRRA - percent_bleached should be classified as above
    #  *FRRP should be classified as above. use the .csv file if you want 2005-2022 data. "Bleaching" column and is part of the Atlantic Ocean since florida collected the data?
    #  *McClanahan - percent_bleached should be classified as above
    #  Safaie - Bleaching_Prevalence_Score_LUT should be classified as above

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
# majority of the data is cleaned and retreived from the db by the print_stats function as specific queries are used for each data source and cleaning
reef_data, other_data = print_stats(dbConn)
# close the db connection after getting the data
dbConn.close()

df = pd.DataFrame(reef_data, columns=['Ocean', 'Country', 'Year', 'S1', 'S2', 'S3', 'S4', 'Data_Source'])
print ("Reef Check:", df.shape)

# create a dataframe from the data
df2 = pd.DataFrame(other_data, columns=['Ocean', 'Country', 'Year', 'Data_Source', 'Percent_Bleached'])
print ("Other Sources:", df2.shape)

#  min and max years in df and df2
print("Reef Check Data: ")
print("Min Year:", df['Year'].min())
print("Max Year:", df['Year'].max())
print("Other Sources Data: ")
print("Min Year:", df2['Year'].min())
print("Max Year:", df2['Year'].max())


# lets check if there are any null values and if so lets drop them, although majority of the data is cleaned in the queries by the use of IS NOT NULL
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


# grouped by year and ocean and the mean of the percent bleached column is calculated for each group
# this allows to plot the yearly increase in coral bleaching level by ocean and shows how 
# bleaching levels vary over time across the oceans.
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