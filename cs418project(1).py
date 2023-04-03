# -*- coding: utf-8 -*-
"""CS418Project.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1RQl3nWL7qsy5j1vDnqnv2YleJmY8mE8c
"""

!pip install seaborn

# Commented out IPython magic to ensure Python compatibility.
import pandas as pd
import numpy as np
import seaborn as sns
# %matplotlib inline
import matplotlib.pyplot as plt

from google.colab import drive
drive.mount('/content/drive')

# This dataset contains "Karenia brevis" data from Texas, Mississippi, Alabama, and Florida, 
# as well as data along the Florida Shelf in the Gulf of Mexico and 
# along the eastern coast of Florida in the North Atlantic Ocean.
# Karenia brevis is a harmful algae species

alge_growth = pd.read_csv('habsos_20220225.csv')
        
alge_growth.head(100)

"""Data sources:

https://www.ncei.noaa.gov/maps/habsos/maps.htm

"""

# original data size
print(alge_growth.shape)

#cleaning data

alge_growth_clean = alge_growth.drop(columns=['WIND_DIR', 'WIND_DIR_UNIT', 'WIND_DIR_QA', 'WIND_SPEED', 'WIND_SPEED_UNIT', 'WIND_SPEED_QA'])

# obtaining relavent information from database
alge_growth_clean = alge_growth_clean[alge_growth_clean['CELLCOUNT'] >= 10000] 

# convert SAMPLE_DATE to data and time format 
alge_growth_clean['SAMPLE_DATE'] = pd.to_datetime(alge_growth_clean['SAMPLE_DATE'], format='%d-%b-%y %I.%M.%S.%f000000 %p')
alge_growth_clean = alge_growth_clean[alge_growth_clean['SAMPLE_DATE'] >= '1953-01-01']
alge_growth_clean = alge_growth_clean[alge_growth_clean['SAMPLE_DATE'] <= '2022-12-31']

q = alge_growth_clean['SAMPLE_DATE'].quantile(0.95)
alge_growth_clean = alge_growth_clean[alge_growth_clean['SAMPLE_DATE'] <= q]


print(alge_growth_clean.info())
print(alge_growth_clean.shape)

# Cell count VS state ID
year = alge_growth_clean['SAMPLE_DATE'].dt.year
sns.scatterplot(data=alge_growth_clean, x=year , y='CELLCOUNT', hue='STATE_ID')
plt.show()

alge_growth_clean_y = alge_growth_clean
alge_growth_clean_y['YEAR'] = alge_growth_clean['SAMPLE_DATE'].dt.year

# group the data by year and calculate the mean of CELLCOUNT
grouped_data = alge_growth_clean_y.groupby(['YEAR', 'STATE_ID'], as_index=False)['CELLCOUNT'].mean()

# create a facet grid plot by year and state
g1 = sns.FacetGrid(grouped_data, col='YEAR', col_wrap=3)
g1.map(sns.barplot, 'STATE_ID', 'CELLCOUNT')

g2 = sns.lineplot(data=grouped_data, x='YEAR', y='CELLCOUNT', hue='STATE_ID')
g2.set_title('Average Cell Count by Year and State')

