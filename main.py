# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---



# +
from gssutils import *
from requests import Session
from cachecontrol import CacheControl
from cachecontrol.caches.file_cache import FileCache
from cachecontrol.heuristics import ExpiresAfter

scraper = Scraper('https://statswales.gov.wales/Catalogue/Local-Government/Finance/Council-Tax/Dwellings/chargeableemptyandsecondhomesbylocalauthority',
                  session=CacheControl(Session(),
                                       cache=FileCache('.cache'),
                                       heuristic=ExpiresAfter(days=7)))
scraper
# -

if len(scraper.distributions) == 0:
    from gssutils.metadata import Distribution
    dist = Distribution(scraper)
    dist.title = 'Dataset'
    dist.downloadURL = 'http://open.statswales.gov.wales/dataset/LGFS0001'
    dist.mediaType = 'application/json'
    scraper.distributions.append(dist)
table = scraper.distribution(title='Dataset').as_pandas()
table

table.columns

cols = {
    'Authority_AltCode1': 'Geography',
    'Data': 'Value',
    'Row_Code': 'Chargeable homes',
    'Year_Code': 'Period'
}
to_remove = set(table.columns) - set(cols.keys())
table.rename(columns=cols, inplace=True)
table.drop(columns=to_remove, inplace=True)
table

table['Period'] = table['Period'].map(lambda x: f'gregorian-interval/{str(x)[:4]}-03-31T00:00:00/P1Y')
table['Measure Type'] = 'Count'
table['Unit'] = 'vacancies'

out = Path('out')
out.mkdir(exist_ok=True, parents=True)
table.drop_duplicates().to_csv(out / 'observations.csv', index = False)

schema = CSVWSchema('https://ons-opendata.github.io/ref_housing/')
schema.create(out / 'observations.csv', out / 'observations.csv-schema.json')

from datetime import datetime
scraper.dataset.family = 'housing'
scraper.dataset.theme = THEME['housing-planning-local-services']
scraper.dataset.modified = datetime.now()
scraper.dataset.creator = scraper.dataset.publisher
with open(out / 'dataset.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

table

