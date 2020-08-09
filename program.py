# -*- coding: utf-8 -*-

import pandas as pd
import xml.etree.ElementTree as etree
import codecs
import csv
import time
import os
import gzip
import sqlalchemy

#Define variables
PATH_FILE = '' #Compile with the path where you have the file downloaded
FILENAME_METADATA = 'movies_metadata.csv'
FILENAME_XML = 'enwiki-latest-abstract.xml.gz'
FILENAME_ABSTRACT = 'abstract.csv'
ENCODING = "utf-8"
POSTGRES_CONNECTION = "postgresql://username:password@host:port/database" #Compile with the connection credentials

N_ROWS_KEEP = 1000
totalCount = 0
abstractCount = 0
title = ''
url = ''
abstract = ''

pathMetadata = os.path.join(PATH_FILE, FILENAME_METADATA)
pathXML = os.path.join(PATH_FILE, FILENAME_XML)
pathAbstract = os.path.join(PATH_FILE, FILENAME_ABSTRACT)

def strip_tag_name(t):
    t = elem.tag
    idx = k = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


input_data = gzip.open(pathXML, 'rb')

#Streaming the gz XML to extract only the necessary informations
with codecs.open(pathAbstract, "w", ENCODING) as abstractFH: 
    abstractWriter = csv.writer(abstractFH, quoting=csv.QUOTE_MINIMAL)
   
    abstractWriter.writerow(['title', 'url', 'abstract'])


    for event, elem in etree.iterparse(input_data, events=('start', 'end')):
        tname = strip_tag_name(elem.tag)
    
        if event == 'start':
            if tname == 'doc':
                title = ''
                url = ''
                abstract = ''
        else:
            if tname == 'title':
                #Deleting the initial part that is always 'Wikipedia:'
                title = elem.text[11:] 
            elif tname == 'url': 
                url = elem.text
            elif tname == 'abstract':
                abstract = elem.text
                
            elif tname == 'doc':
                totalCount += 1
            
                if len(title) > 0:
                    abstractCount += 1
                    abstractWriter.writerow([title, url, abstract])
            
                if totalCount > 1 and (totalCount % 1000) == 0:
                                print("{:,}".format(totalCount))

    elem.clear()
    

#Read the 2 dataset with pandas
wiki_db = pd.read_csv(pathAbstract)
imdb_db = pd.read_csv(pathMetadata)

#Most of the time the title of a movie in Wikipedia has '(film)' in the end
imdb_db['title_2'] = imdb_db['title'] + " (film)"

imdb_db['year'] = pd.DatetimeIndex(imdb_db['release_date']).year
imdb_db['rating'] = imdb_db['vote_average']

#Ratio calculation
imdb_db['ratio'] = imdb_db.revenue.div(imdb_db.budget)

#Assuming when budget or revenue is equal to zero the information is missing
#and is impossible to calculate correctly the ratio. Forced to -1
imdb_db.loc[(imdb_db['budget'] == 0) | (imdb_db['revenue'] == 0) , 'ratio'] = -1

#Dropping unuseful columns
imdb_db.drop(['id', 'imdb_id', 'original_title', 'adult', \
              'belongs_to_collection', 'genres', 'homepage', \
              'original_language', 'overview', 'popularity', 'poster_path', \
              'production_countries', 'runtime', 'spoken_languages', \
              'status', 'tagline', 'video', 'vote_count', 'release_date', \
              'vote_average'], axis=1, inplace=True)

#Sorting by ratio and reset index
imdb_db.sort_values(by='ratio', ascending=False, inplace=True)
imdb_db = imdb_db.reset_index(drop=True)

#Join together the 2 datasets
#First join try to search if exist the page in Wikipedia with '(film)' in the end of the title
final_join = pd.merge(imdb_db, wiki_db, how='left', left_on='title_2', right_on='title')

#Second join try to search if exists the page in Wikipedia without the '(film)' in the end of the title
final_join_2 = pd.merge(final_join, wiki_db, how='left', left_on='title_x', right_on='title')

#Keep the first value founded
final_join_2.loc[pd.isnull(final_join_2['url_x']), 'url_x'] = final_join_2['url_y']
final_join_2.loc[pd.isnull(final_join_2['abstract_x']), 'abstract_x'] = final_join_2['abstract_y']


#Dropping unuseful columns
final_join_2.drop(['title', 'title_2', 'title_y', 'url_y', 'abstract_y'], axis=1, inplace=True)

#Rename columns
final_join_2 = final_join_2.rename(columns = {'url_x':'url', 'abstract_x':'abstract', 'title_x':'title'})

#Final table ready to load on Postgres
final_table = final_join_2.head(N_ROWS_KEEP)


#Storing the final table to Postgres
engine = sqlalchemy.create_engine(POSTGRES_CONNECTION)
con = engine.connect()

table_name = 'Final_Table'
final_table.to_sql(table_name, con)

con.close()




