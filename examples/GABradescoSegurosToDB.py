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
						dbWritePartitions=None,
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
			dbWritePartitions=dbWritePartitions,
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
						dbWritePartitions=None,
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
    
            # Operational and data join
            
            {
                'title': 'hit_id',
                'name': 'ga:dimension22',
                'key': True
            },
    
            {
                'title': 'session_id',
                'name': 'ga:dimension21',
                'key': True
            },

            {
                'title': 'client_id',
                'name': 'ga:dimension23',
                'key': True
            },

            {
                'title': 'user_id',
                'name': 'ga:dimension24',
                'key': True
            },

            {
                'title': 'sequence_id',
                'name': 'ga:dimension25',
                'key': True
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
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
            {
                'title': 'corretor_cpfcnpj_session_sha256',
                'name': 'ga:dimension17',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_zeropad_sha256',
                'name': 'ga:dimension19',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_sha256',
                'name': 'ga:dimension15',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
    
    
    
            # Outros
        
            {
                'title': 'ramo',
                'name': 'ga:dimension3',
            },

            {
                'title': 'channel1',
                'name': 'ga:dimension4',
            },

            {
                'title': 'channel2',
                'name': 'ga:dimension5',
            },
            
            {
                'title': 'product',
                'name': 'ga:dimension14',
            },
            
            {
                'title': 'agency',
                'name': 'ga:dimension26',
            },
            
            {
                'title': 'event_category',
                'name': 'ga:eventCategory',
            },
            
            {
                'title': 'event_action',
                'name': 'ga:eventAction',
            },
			{
                'title': 'event',
                'name': 'ga:eventLabel',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Fix a wrong char that prevents data to be written on DB
                    r'Pesquise por Ano e n.mero do Pedido': 'Pesquise por Ano e número do Pedido',
					r'C.edito': 'Crédito'
                },
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
			dbWritePartitions=dbWritePartitions,
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
						dbWritePartitions=None,
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
            
            
            # Operational and data join
            
            {
                'title': 'hit_id',
                'name': 'ga:dimension22',
                'key': True
            },
    
            {
                'title': 'session_id',
                'name': 'ga:dimension21',
                'key': True
            },

            {
                'title': 'client_id',
                'name': 'ga:dimension23',
                'key': True
            },

            {
                'title': 'user_id',
                'name': 'ga:dimension24',
                'key': True
            },

            {
                'title': 'sequence_id',
                'name': 'ga:dimension25',
                'key': True
            },

    
            # Business visitor features
            
            {
                'title': 'corretor_cpd',
                'name': 'ga:dimension13',
                'type': 'int'
            },

            {
                'title': 'corretor_cpfcnpj_session_zeropad_sha256',
                'name': 'ga:dimension20',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
            {
                'title': 'corretor_cpfcnpj_session_sha256',
                'name': 'ga:dimension17',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_zeropad_sha256',
                'name': 'ga:dimension19',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
            {
                'title': 'corretor_cpfcnpj_hit_sha256',
                'name': 'ga:dimension15',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    'Cookie não definido': None
                },
            },
    
            {
                'title': 'corretor_perfil',
                'name': 'ga:dimension6',
            },
            
    
            # Content
            
            {
                'title': 'page',
                'name': 'ga:pagePath',
            },
            
            {
                'title': 'referer',
                'name': 'ga:fullReferrer',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Get rid of useless strings, replace text by NULL
                    '(direct)': None
                },
            },

            {
                'title': 'pagename',
                'name': 'ga:dimension1',
            },
            
            {
                'title': 'site',
                'name': 'ga:dimension2',
            },

            {
                'title': 'ramo',
                'name': 'ga:dimension3',
            },

            {
                'title': 'channel1',
                'name': 'ga:dimension4',
            },

            {
                'title': 'channel2',
                'name': 'ga:dimension5',
            },
            
            {
                'title': 'product',
                'name': 'ga:dimension14',
            },
            
            {
                'title': 'agency',
                'name': 'ga:dimension26',
            },
            
            {
                'title': 'event_category',
                'name': 'ga:eventCategory',
            },
            
            {
                'title': 'event_action',
                'name': 'ga:eventAction',
            },
            
            {
                'title': 'event_label',
                'name': 'ga:eventLabel',
                'transform': self.TransformRegexReplace, # (report, this_dimension)
                'transformparams': {
                	# Fix a wrong char that prevents data to be written on DB
                    r'Pesquise por Ano e n.mero do Pedido': 'Pesquise por Ano e número do Pedido',
					r'C.edito': 'Crédito'
                },
            },
            


            # Visitor technical features
    
            {
                'title': 'browser',
                'name': 'ga:browser',
                'key': True
            },


            {
                'title': 'browser_version',
                'name': 'ga:browserVersion',
            },

            {
                'title': 'browser_size',
                'name': 'ga:browserSize',
            },
            {
                'title': 'screen_resolution',
                'name': 'ga:screenResolution',
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
			dbWritePartitions=dbWritePartitions,
            emptyRows=emptyRows,
            dbURL=dbURL,
            targetTable=targetTable,
            update=update,
            processorName=processorName,
            restart=restart
        )








