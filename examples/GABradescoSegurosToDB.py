#######################################
##
## Contains classes that implement methods to process our specific dimensions.
## These classes also define the list of dimensions they'll work with.
## No configuration, GA credentials or DB details are defined here. Create an external
## executable script that instantiate this classes for this purpose.
##
## Written by Avi Alkalay <avi at unix dot sh>
## May 2020
##

import pandas as pd
from GAAPItoDB import GAAPItoDB
import logging.handlers
import logging
import argparse
import pprint
import datetime
import numpy as np
import os


module_logger = logging.getLogger(__name__)


# A base abstract class that contains custom methods to process and transform our private dimensions.
# All general logic is defined in GAAPItoDB class.
class GABradescoSegurosToDB(GAAPItoDB):
    def __init__(
                        self,
                        gaView,
                        dimensions,
                        start,
                        credentialsFile,
                        gaAccount=None,
                        gaProperty=None,
                        gaTimezone=None,  # a timezone name as 'America/Sao_Paulo' or 'Etc/GMT'
                        end=None,
                        incremental=True,
                        endLag=None,
                        apiQuota=0,
                        metrics=None,
                        dateRangePartitionSize=None,  # in days
                        emptyRows=True,
                        dbURL=None,
                        targetTable=None,
                        update=True,
                        processorName=None,
                        restart=False
        ):
        super().__init__(
            gaView=gaView,
            dimensions=dimensions,
            start=start,
            credentialsFile=credentialsFile,
            gaAccount=gaAccount,
            gaProperty=gaProperty,
            gaTimezone=gaTimezone,  # a timezone name as 'America/Sao_Paulo' or 'Etc/GMT'
            end=end,
            incremental=incremental,
            endLag=endLag,
            apiQuota=apiQuota,
            metrics=metrics,
            dateRangePartitionSize=dateRangePartitionSize,  # in days
            emptyRows=emptyRows,
            dbURL=dbURL,
            targetTable=targetTable,
            update=update,
            processorName=processorName,
            restart=restart
        )



    # Define custom transformation methods specific to Bradesco Seguros's case


    def BSExplodeAndSplit(self, df, dimension):
        if 'keeporiginal' in dimension:
            # Keep original data in a new column with suffix "__org"
            orgname = dimension['title'] + "__org"
            df[orgname] = df[dimension['title']]

        for s in dimension['transformparams']['explodeseparators']:    
    
            self.logger.debug(f"BSExplodeAndSplit: exploding {dimension['title']} on {s}")
        
            try:
                # convert values of 'column' as "a-1,b-2,c-3" -> ["a-1","b-2","c-3"]
                df[dimension['title']] = df[dimension['title']].str.split(pat = s)
        
                # convert ["a-1","b-2","c-3"] into multiple rows duplicating values of other columns
                df = df.explode(dimension['title'])
            except Exception as e:
                self.logger.error("BSExplodeAndSplit: unrecoverable error in custom transformation: " + e)
                os._exit(1)    



        # Last separator will split into multiple columns
        try:
            self.logger.debug(f"BSExplodeAndSplit: splitting {dimension['title']} on {dimension['transformparams']['splitseparators'][0]}")    
            df[dimension['transformspawncolumns']] = df[dimension['title']].str.split(dimension['transformparams']['splitseparators'][0], expand=True)
        except Exception as e:
            self.logger.error("BSExplodeAndSplit: unrecoverable error in custom transformation: " + e)
            os._exit(1)    
    
        # Now convert types
    
        if 'transformspawncolumnstype' in dimension and 'transformspawncolumns' in dimension:
            for i in range(len(dimension['transformspawncolumns'])):
                self.logger.debug(f"BSExplodeAndSplit: log something 2")
                if dimension['transformspawncolumnstype'][i]:
                    if dimension['transformspawncolumnstype'][i] == 'int':
                        df[dimension['transformspawncolumns'][i]] = pd.to_numeric(df[dimension['transformspawncolumns'][i]],errors='raise')
                    if dimension['transformspawncolumnstype'][i] == 'datetime':
                        df[dimension['transformspawncolumns'][i]] = pd.to_datetime(df[dimension['transformspawncolumns'][i]])

                        # Add GA View's time zone just to convert time to UTC right away
                        df[dimension['transformspawncolumns'][i]]=df[dimension['transformspawncolumns'][i]].apply(
                            lambda x: x.tz_localize(self.gaTimezone).tz_convert(None)
                        )


                        
                        
        self.logger.debug(f"BSExplodeAndSplit: end of custom transformation.")
        return df










    def BSNullifyStrings(self, df, dimension):
        if 'keeporiginal' in dimension:
            # Keep original data in a new column with suffix "__org"
            orgname = dimension['title'] + "__org"
            df[orgname] = df[dimension['title']]

        for s in dimension['transformparams']['nullify']:
            self.logger.debug(f"BSNullifyStrings: nullifying «{s}» on {dimension['title']}")
            try:
                df[dimension['title']]=df[[dimension['title']]].replace(s, np.nan)
            except Exception as e:
                self.logger.error("BSExplodeAndSplit: unrecoverable error in custom transformation: " + e)
                os._exit(1)    

        self.logger.debug(f"BSNullifyStrings: end of custom transformation.")
        return df
    
























# A class that simply defines dimensions to consume from GA.
# All custom logic is defined in GABradescoSegurosToDB parent class.
# All general logic is defined in GAAPItoDB class.
class GABradescoSegurosParcelasAtrasadasClicadasToDB(GABradescoSegurosToDB):
    def __init__(
                        self,
                        gaView,
                        start,
                        credentialsFile,
                        gaAccount=None,
                        gaProperty=None,
                        gaTimezone=None,  # a timezone name as 'America/Sao_Paulo' or 'Etc/GMT'
                        end=None,
                        incremental=True,
                        endLag=None,
                        apiQuota=0,
                        metrics=None,
                        dateRangePartitionSize=None,  # in days
                        emptyRows=True,
                        dbURL=None,
                        targetTable=None,
                        update=True,
                        processorName=None,
                        restart=False
        ):
        
        
        dimensions = [
            {
                'title': 'utc_datetime',
                'name': 'ga:dateHourMinute',
                'type': 'datetime',
                'synccursor': True,
                'key': True,
                'sort': True,
                'keeporiginal': True
            },
    
    
    
            # Sucursal e Apólice
    
            {
                'title': 'sucursal_apolice',
                'name': 'ga:dimension7',
                'key': True,
                'keeporiginal': True,
                'transform': self.BSExplodeAndSplit, # (report, this_dimension)
                'transformspawncolumns': ['sucursal','apolice'],
                'transformspawncolumnstypes': ['int','int'],
                'transformparams': {
                    'explodeseparators': [','],
                    'splitseparators': ['-'],
                }
            },
            {
                'title': 'corretor_sucursal',
                'name': 'ga:dimension11',
                'type': 'int'
            },
            {
                'title': 'apolice_isolada',
                'name': 'ga:dimension10',
                'type': 'int'
            },

    
    
            # Dados do cliente
    
            {
                'title': 'cliente_cpfcnpj_zeropad_sha256',
                'name': 'ga:dimension18'
            },
            {
                'title': 'cliente_cpfcnpj_sha256',
                'name': 'ga:dimension12'
            },
    
    

            # Dados do corretor
    
            {
                'title': 'corretor_cpd',
                'name': 'ga:dimension13',
                'type': 'int'
            },
            {
                'title': 'corretor_cpfcnpj_session_zeropad_sha256',
                'name': 'ga:dimension20',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
            {
                'title': 'corretor_cpfcnpj_session_sha256',
                'name': 'ga:dimension17',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },        
            },
            {
                'title': 'corretor_cpfcnpj_hit_zeropad_sha256',
                'name': 'ga:dimension19',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_sha256',
                'name': 'ga:dimension15',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
    
    
    
            # Outros
        
            {
                'title': 'event',
                'name': 'ga:eventLabel',
            }
        ]


        
        super().__init__(
            gaView=gaView,
            gaAccount=gaAccount,
            gaProperty=gaProperty,
            gaTimezone=gaTimezone,
            dimensions=dimensions,
            start=start,
            credentialsFile=credentialsFile,
            end=end,
            incremental=incremental,
            endLag=endLag,
            apiQuota=apiQuota,
            metrics=metrics,
            dateRangePartitionSize=dateRangePartitionSize,  # in days
            emptyRows=emptyRows,
            dbURL=dbURL,
            targetTable=targetTable,
            update=update,
            processorName=processorName,
            restart=restart
        )





# A class that simply defines dimensions to consume from GA.
# All custom logic is defined in GABradescoSegurosToDB parent class.
# All general logic is defined in GAAPItoDB class.

class GABradescoSegurosCorretorVisitanteToDB(GABradescoSegurosToDB):
    def __init__(
                        self,
                        gaView,
                        start,
                        credentialsFile,
                        gaAccount=None,
                        gaProperty=None,
                        gaTimezone=None,  # a timezone name as 'America/Sao_Paulo' or 'Etc/GMT'
                        end=None,
                        incremental=True,
                        endLag=None,
                        apiQuota=0,
                        metrics=None,
                        dateRangePartitionSize=None,  # in days
                        emptyRows=True,
                        dbURL=None,
                        targetTable=None,
                        update=True,
                        processorName=None,
                        restart=False
        ):
        
        dimensions = [
            {
                'title': 'utc_datetime',
                'name': 'ga:dateHourMinute',
                'type': 'datetime',
                'synccursor': True,
                'key': True,
                'sort': True,
                'keeporiginal': True
            },
    

    
            # Dados do corretor
    
            {
                'title': 'corretor_cpfcnpj_session_zeropad_sha256',
                'name': 'ga:dimension20',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
            {
                'title': 'corretor_cpfcnpj_session_sha256',
                'name': 'ga:dimension17',
                'key': True,
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_zeropad_sha256',
                'name': 'ga:dimension19',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_sha256',
                'name': 'ga:dimension15',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
    
    
    
            # Outros
    
            {
                'title': 'browser',
                'name': 'ga:browser',
                'key': True
            },


            {
                'title': 'browser_version',
                'name': 'ga:browserVersion',
                'key': True
            },

            {
                'title': 'browser_size',
                'name': 'ga:browserSize',
                'key': True
            },
            {
                'title': 'screen_resolution',
                'name': 'ga:screenResolution',
                'key': True
            },
            {
                'title': 'referer',
                'name': 'ga:fullReferrer',
                'key': True
            },

            {
                'title': 'page',
                'name': 'ga:pagePath',
                'key': True
            },
        ]

  
        
        super().__init__(
            gaView=gaView,
            gaAccount=gaAccount,
            gaProperty=gaProperty,
            gaTimezone=gaTimezone,
            dimensions=dimensions,
            start=start,
            credentialsFile=credentialsFile,
            end=end,
            incremental=incremental,
            endLag=endLag,
            apiQuota=apiQuota,
            metrics=metrics,
            dateRangePartitionSize=dateRangePartitionSize,  # in days
            emptyRows=emptyRows,
            dbURL=dbURL,
            targetTable=targetTable,
            update=update,
            processorName=processorName,
            restart=restart
        )








