#!/usr/bin/env python3

#######################################
##
## Executable script that glues all together and execute ETL.
## Prepare logging, instantiate objects of classes defined in GABradescoSegurosToDB,
## provide GA credentials, DB connection URL, target table name etc.
##
## Written by Avi Alkalay <avi at unix dot sh>
## May 2020
##


import logging.handlers
import logging
import argparse
import pprint
import datetime
import GABradescoSegurosToDB


# Prepare logging

# Switch between INFO/DEBUG while running in production/developping:
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())




# Display start time

logging.getLogger().debug(datetime.datetime.now())



# Set GA credentials file obtained from https://console.developers.google.com/apis/dashboard

credentials='Portal de Negocios-abc123.json'




# Instantiate our specialized classes.
# We have 2 views:
#   - an older regular one that has more data (suffix TZ)
#   - a new view, created after learning about GA good practices, identical to the former
#     but configured to record time as UTC, not localtime. All new data should be fetched
#     from the UTC view.
#
# Notice how TZ objects have specific end time (end=datetime.datetime(2020,4,22,0,0,0))
# while UTC objects don't. This is an indication that end-limited objects should not get
# data after end date becase since then we correctly configured GA to write data also in
# the UTC view.
#
# Also notice that UTC objects have start time (start=datetime.datetime(2020,4,22)) that
# matches his TZ sibling end time.

# Parameters are:
#
# - gaView, gaAccount, gaProperty: self explanatory
# - gaTimezone: A timezone name as 'America/Sao_Paulo' or 'Etc/GMT'. A hint on
#   which timezone your gaView is configured so the ETL can transform it correctly yo UTC.
#   Leave it as None to let the class discover from your View. Always, always, prefer GA
#   Views configured as 'Etc/GMT'.
# - credentialsFile: JSON file with GA credentials and API keys as provided Google
# - apiQuota: Number of API calls per 100 seconds that Google allows your credentials
#   to make. ETL logic will pause for a while if this quota is achieved, to avoid an API error.
# - star, end: Python datetime objects that defines date boundaries which has the desired dimensions.
#   If end is not specified, grab data until now. The start parameter can't be omitted.
# - endLag: Grab GA data produced until end time minus endLag period. This is useful when
#   you sync several times per day and need to give GA some time to process your data,
#   guaranteed that GA had a time to process your data.
# - dateRangePartitionSize: Number of days to ask data to Google. More days will run
#   faster and require less API calls, but increase your chance of getting
#   sampled (incomplete) data. Less days ensures complete data but makes more API calls
#   and takes longer times to run.
# - dbURL: A SQLAlchemy-supported URL including user, password, host and database name.
# - targetTable: SQL table name that will be updated.
# - incremental: If True (default) get data from GA and update SQL table starting from
#   most recent hit found in table.
# - update: If False, do everything except write data in DB. Default is True.
# - restart: Reset data tables (targetTable) and grab GA data since the beginning (start).
#   Default is False.


gaParcelasClicadasTZ = GABradescoSegurosToDB.GABradescoSegurosParcelasAtrasadasClicadasToDB(
    gaView=199999996,
    gaAccount=79999999,
    gaProperty='UA-79999999-17',    
    credentialsFile=credentials,
    apiQuota=100,
    start=datetime.datetime(2019,7,9),
    end=datetime.datetime(2020,4,22,0,0,0),
    endLag=datetime.timedelta(minutes=0),
    dateRangePartitionSize=7,
    dbURL="mysql://user:password@host.com/database?charset=utf8mb4",
    targetTable='ga_parcelas_atrasadas_clicadas'
)

gaParcelasClicadasUTC = GABradescoSegurosToDB.GABradescoSegurosParcelasAtrasadasClicadasToDB(
    gaView=199999996,
    gaAccount=79999999,
    gaProperty='UA-79999999-17',    
    credentialsFile=credentials,
    apiQuota=100,
    start=datetime.datetime(2020,4,22),
    endLag=datetime.timedelta(minutes=120),
    dateRangePartitionSize=7,
    dbURL="mysql://user:password@host.com/database?charset=utf8mb4",
    targetTable='ga_parcelas_atrasadas_clicadas'
)








# The following dimensions return a huge amount of data (aprox 800k lines per day), so
# we'll decrease dateRangePartitionSize to only 2 days, to avoid GA sampling (incomplete data).

gaCorretorVisitanteTZ=GABradescoSegurosToDB.GABradescoSegurosCorretorVisitanteToDB(
    gaView=199999996,
    gaAccount=79999999,
    gaProperty='UA-79999999-17',    
    credentialsFile=credentials,
    apiQuota=100,
    start=datetime.datetime(2020,3,1),
    end=datetime.datetime(2020,4,22,0,0,0),
    endLag=datetime.timedelta(minutes=0),
    dateRangePartitionSize=2,
    dbURL="mysql://user:password@host.com/database?charset=utf8mb4",
    update=True,
    targetTable='ga_corretor_visitante'
)


gaCorretorVisitanteUTC=GABradescoSegurosToDB.GABradescoSegurosCorretorVisitanteToDB(
    gaView=199999996,
    gaAccount=79999999,
    gaProperty='UA-79999999-17',    
    credentialsFile=credentials,
    apiQuota=100,
    start=datetime.datetime(2020,4,22),
    endLag=datetime.timedelta(minutes=120),
    dateRangePartitionSize=2,
    dbURL="mysql://user:password@host.com/database?charset=utf8mb4",
    update=True,
    targetTable='ga_corretor_visitante'
)




# End of configuration. Now execute ETL to fetch latest data from GA and write in DB.
# DB will be checked to see what was last data written and sync will start from there.
# So it is safe and efficient to cancel this script in the middle of execution and
# simply restart it.

logging.getLogger().debug(datetime.datetime.now())
gaParcelasClicadasTZ.sync()
del gaParcelasClicadasTZ

logging.getLogger().debug(datetime.datetime.now())
gaParcelasClicadasUTC.sync()
del gaParcelasClicadasUTC

logging.getLogger().debug(datetime.datetime.now())
gaCorretorVisitanteTZ.sync()
del gaCorretorVisitanteTZ

logging.getLogger().debug(datetime.datetime.now())
gaCorretorVisitanteUTC.sync()
del gaCorretorVisitanteUTC

logging.getLogger().debug(datetime.datetime.now())
