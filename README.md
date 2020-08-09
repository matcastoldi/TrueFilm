# TrueFilm
 
To run the program you need to install:
- Python (I used the latest version available now 3.8.5)
- Postgres (I used 12.3.2)

Python libraries needed:
- pandas
- xml.etree.ElementTree
- codecs
- csv
- time
- os
- gzip
- sqlalchemy


I choosed to use Python because it's a programming language that make easy manipulate the data, specially with Pandas library.
I decide to manipulate the XML file dump of Wikipedia without decompressing it to keep the process faster and to not import directly in Python as XML.
The XML file is streamed to a csv file to keep only the necessary informations, in this case: title, URL of Wikipedia page and abstract.

To check the correctness I first check if the amount of row imported from IMDB file was the same included in the file, then I checked a sample of row of the final table to control if the information was joined correctly.

