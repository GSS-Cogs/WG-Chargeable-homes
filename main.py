# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
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

# +
#table.columns
# -

cols = {
    'Authority_AltCode1': 'Geography',
    'Data': 'Value',
    'Row_Code': 'Chargeable homes',
    'Year_Code': 'Period',
    'Band_ItemName_ENG': 'Council tax band'
}
to_remove = set(table.columns) - set(cols.keys())
table.rename(columns=cols, inplace=True)
table.drop(columns=to_remove, inplace=True)

# The OData API offers an "Items" endpoint that enumerates the values of the various dimensions and provides information about the hierarchy.

items_dist = scraper.distribution(title='Items')
items = items_dist.as_pandas()
#display(items)
#items['DimensionName_ENG'].unique()

# +
from collections import OrderedDict
item_cols = OrderedDict([
    ('Description_ENG', 'Label'),
    ('Code', 'Notation'),
    ('Hierarchy', 'Parent Notation'),
    ('SortOrder', 'Sort Priority')
])

def extract_codelist(dimension):
    codelist = items[items['DimensionName_ENG'] == dimension].rename(
        columns=item_cols).drop(
        columns=set(items.columns) - set(item_cols.keys()))[list(item_cols.values())]
    codelist['Notation'] = codelist['Notation'].map(
        lambda x: str(int(x)) if str(x).endswith(".0") else str(x)
    )
    return codelist

codelists = {
    'chargeable-homes': extract_codelist('Row'),
    'council-tax-bands': extract_codelist('Band')
}

out = Path('out')
out.mkdir(exist_ok=True, parents=True)

for name, codelist in codelists.items():
    codelist.to_csv(out / f'{name}.csv', index = False)
    display(name)
    display(codelist)

# +
table['Period'] = table['Period'].map(lambda x: f'gregorian-interval/{str(x)[:4]}-03-31T00:00:00/P1Y')
table['Measure Type'] = 'Count'
table['Unit'] = 'vacancies'

table['Council tax band'] = table['Council tax band'].map(
    lambda x: {
        'A-' : 'Adash',
        'Total' : 'total'
        }.get(x, x))
# -

out = Path('out')
out.mkdir(exist_ok=True, parents=True)
table.drop_duplicates().to_csv(out / 'observations.csv', index = False)

#schema = CSVWMetadata('https://ons-opendata.github.io/ref_housing/')
schema = CSVWMetadata('https://gss-cogs.github.io/ref_housing/')
schema.create(out / 'observations.csv', out / 'observations.csv-schema.json')

from datetime import datetime
scraper.dataset.family = 'housing'
scraper.dataset.theme = THEME['housing-planning-local-services']
scraper.dataset.modified = datetime.now()
scraper.dataset.creator = scraper.dataset.publisher
with open(out / 'dataset.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

# +
#table['Geography'].unique()

# +
#table['Council tax band'].unique()

# +
#table['Chargeable homes'].unique()
#table.head
