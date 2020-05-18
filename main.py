# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Constants
#
# We'll need to set some to account for family etc
#

# +
from gssutils import *

theme_is = THEME["housing-planning-local-services"]         # note - is it?
family_ref_is = 'https://gss-cogs.github.io/ref_housing/'   # note - is it?
# -

#
# ## Scrape The Data
#
# This is a bit more compicated that normal but it's just to help with site reliability (i.e it's flakey so we're caching the scrape when we get it - in case it's down the next time we run). 

# +

import csv
import shutil
import json

import requests
from cachecontrol import CacheControl
from cachecontrol.caches.file_cache import FileCache
from cachecontrol.heuristics import ExpiresAfter

# Scrape the data
# the 'cache' stuff helps with the generally unreliability of the site
scraper = Scraper('https://statswales.gov.wales/Catalogue/Local-Government/Finance/Council-Tax/Dwellings/chargeableemptyandsecondhomesbylocalauthority',
                  session=CacheControl(requests.Session(), cache=FileCache('.cache'), heuristic=ExpiresAfter(days=7)))
                                       
scraper
# -

# # These are NOT all distibutions ..
#
# They're the dataset plus supporting it's metadata files, the existing "distribution scraping" mechanic is just a convienient way of getting everything. 

# -----
#
# # Create local reference data
#
# The 'Items' file contains all the codes used by the dataset as well as the dimension they come under. We're gonna use this to create the required codelists for this dataset.
#
# For convenience we'll start with a preview of this 'Items' data as a csv, I'll use the shorthand 'df' a lot in this script btw, df is dataframe (which in python means pandas in this and almost all cases).

items_df = scraper.distribution(title='Items').as_pandas()  # get the data into a dataframe
items_df[:5] # show me the first 5 lines

# The name of the dimension is in the column 'DimensionName_Eng', 'Notes_ENG' is what we'd call description and Code, SortOrder and Hierarchy are where you'd think.
#
# We're going to out the reference data to a local directory called /reference

# +

# Delete old local reference data to start - or things will get hard to follow
shutil.rmtree('reference')

# Make sure a reference/codelists directory exists if it does not
codelist_dir = Path('reference/codelists')
codelist_dir.mkdir(exist_ok=True, parents=True)
reference_dir = Path('reference')

# Add some skeletal files we can append to as we go
col_path = Path(reference_dir / "columns.csv")
with open(col_path, 'w') as f:
    f.write('title,name,component_attachment,property_template,value_template,datatype,value_transformation,regex,range\n')

comp_path = Path(reference_dir / "components.csv")
with open(comp_path, 'w') as f:
    f.write('Label,Description,Component Type,Codelist,Path,Range,Parent\n')

meta_path = Path(reference_dir / "codelists-metadata.json")
with open(meta_path, 'w') as f:
    f.write(json.dumps({"@context": ["http://www.w3.org/ns/csvw",{"@language": "en"}],"tables": []}))

# For every unique item in the "dimensionName_Eng" column
for dimension_name in items_df["DimensionName_ENG"].unique().tolist():
    
    # Slice so we've only got that dimension
    df_slice = items_df[items_df["DimensionName_ENG"] == dimension_name]
    
    # Create a new dataframe and map their columns to our names for the same
    # left side - our name for things, right side - their names for things
    codelist_df = pd.DataFrame()
    codelist_df["Label"] = df_slice["Description_ENG"]
    codelist_df["Notation"] = df_slice["Code"]
    codelist_df["Parent Notation"] = df_slice["Hierarchy"]
    codelist_df["Sort Priority"] = df_slice["SortOrder"]
    codelist_df["Description"] = df_slice["Notes_ENG"]
    
    # Output the codelist
    codelist_df.to_csv(f'reference/codelists/{pathify(dimension_name)}.csv', index=False)
    
    # Add a line to columns.csv for this dimension
    with open(col_path, "a") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow([dimension_name,
                    pathify(dimension_name).replace("-", "_"),
                    "qb:dimension",
                    "http://gss-data.org.uk/def/dimension/{}".format(pathify(dimension_name)),
                    "http://gss-data.org.uk/def/dimension/{}/{}".format(pathify(dimension_name), pathify(dimension_name.replace('-', '_'))),
                    "string",
                    "slugize"])
    
    # Add a line to components.csv for this dimension
    with open(comp_path, "a") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow([dimension_name,
                        "",
                        "Dimension",
                        "http://gss-data.org.uk/def/concept-scheme/{}".format(pathify(dimension_name)),
                        "dimension/{}".format(pathify(dimension_name)),
                        dimension_name.replace(" ", "").title(),
                        ""])
                        
    
    # Add an entry to codelist-metadata.json for this dimension
    data = None
    with open(meta_path, "r") as f:
        data = json.load(f)
        data["tables"].append({
            "url": "codelists/{}.csv".format(pathify(dimension_name)),
            "tableSchema": "https://gss-cogs.github.io/ref_common/codelist-schema.json",
            "rdfs:label": dimension_name 
        })
    with open(meta_path, "w") as f:
        json.dump(data, f)
    
# -

# -----
# # Incorporate metadata
#
# There's a few things we could add from the metadata endpoint (printed below) but for now I'm just taking the contact point ("Contact email" in the below table).

meta_df = scraper.distribution(title='Metadata').as_pandas()
scraper.dataset.contactPoint = meta_df[meta_df["Tag_ENG"] == "Contact email"]["Description_ENG"].values[0]
meta_df

# -----
#
# # Create the observations file, output it along with trig file etc

obs_df = scraper.distribution(title='Dataset').as_pandas()
obs_df[:5] # first 5 lines

# first the observation file ...

# +
# Remove any columns that (a) are not the 'Data' (Value to us) column and (b) dont end with 'code' 
for col in [x for x in obs_df.columns.values if not x.lower().endswith("code") and x != "Data"]:
    obs_df = obs_df.drop(col, axis=1)
    print("Dropped column {}.".format(col))

# Sort out the column names (we're making a dictionary of {"old name1": "new name1"} etc)
nice_col_names = { col: col.split("_")[0] for col in [x for x in obs_df.columns.values if x.lower().endswith("code")] }
nice_col_names["Data"] = "Value"
obs_df = obs_df.rename(columns=nice_col_names)
print("Mapped to new column names as:\n", str(nice_col_names))

out = Path('out')
out.mkdir(exist_ok=True, parents=True)
obs_df.drop_duplicates().to_csv(out / 'observations.csv', index = False)
# -

#
# <br>
#
# then output the schema etc ...

schema = CSVWMetadata(family_ref_is)
schema.create(out / 'observations.csv', out / 'observations.csv-schema.json')
with open(out / 'observations.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
