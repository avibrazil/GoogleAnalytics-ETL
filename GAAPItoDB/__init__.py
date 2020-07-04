#######################################
##
## The GAAPItoDB class that has all generic multi-threading logic to get a list of
## desired GA dimension data, transform them to SQL-ready data and write them to a
## database table in chunks and in parallel.
##
## Written by Avi Alkalay <avi at unix dot sh>
## May 2020
##



from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import logging
import datetime
import dateutil.parser
import copy
import pandas as pd
import io
import hashlib
import sqlalchemy
import time
import json
import queue
import threading
import gc # Garbage Collector


module_logger = logging.getLogger(__name__)

class GAAPItoDB(object):
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
        """
        Get report data between `start` and `end` times.
        
        If `incremental` is True, use start date as the last date and time that appears in the `targetTable`, column of dimension set with `synccursor`.
        
        If `restart` is True, ignore `incremental` and use `start` date.
        
        Sync data to database from GA up to `end` date minus `endLag`. If not set, `endLag` will be 30 minutes. The `endLag` is important so you won't get too hot and unprocessed data from GA.
        """
        # Setup logging
        if __name__ == '__main__':
            self.logger=logging.getLogger('{a}.{b}'.format(a=type(self).__name__, b=type(self).__name__))
        else:
            self.logger=logging.getLogger('{a}.{b}'.format(a=__name__, b=type(self).__name__))
        
        
        self.gaView=f'{gaView}' # force conversion to string
        self.gaAccount=f'{gaAccount}' # force conversion to string
        self.gaProperty=f'{gaProperty}' # force conversion to string
        self.credentialsFile=credentialsFile

        self.dimensions=dimensions
        self.metrics=metrics
        self.dbURL=dbURL
        self.targetTable=targetTable
        self.dateRangePartitionSize=dateRangePartitionSize
        self.quotaControl={}
        self.restart=restart
        self.update=update
        self.apiQuota=apiQuota

        self.start=start
        self.end=end
        self.incremental=incremental
        self.endLag=endLag
        
        self.ga=None
        
        self.ga=self.getGA()  # Use credentials to get a Google Aalytics object
        
        
        
        # Need GA View's timezone to convert all times to UTC
        self.gaViewObject=self.getGAViewObject()
        
        if gaTimezone is not None:
            # Passed in constructor
            self.gaTimezone=gaTimezone
        else:
            # Lets discover timezone configured for this GA View
            self.gaTimezone=self.gaViewObject['timezone']

        # Show some info about timezone, causae its an important subject
        t=pd.Timestamp.now()
        self.logger.warning("This GA View's time zone is «{tz}». A timestamp as «{example}» will be handled as «{example_converted}» and stored in database in UTC format, as «{utc}».".format(
            tz=self.gaTimezone,
            example=t.isoformat(),
            example_converted=t.tz_localize(self.gaTimezone).isoformat(),
            utc=t.tz_localize(self.gaTimezone).tz_convert(None).isoformat()
        ))

            

        if self.end is None:
            # If not defined by the user, get an end time as now() in GA's timezone.
            # now() gets you to the running server local time (and timezone), which might be different from GA's timezone,
            # so this server now() might be much in the future for GA, which might anihilate all the purpose of an endLag.
            self.end=pd.Timestamp.utcnow().tz_convert(self.gaTimezone).to_pydatetime()

            
        
        if self.endLag is None:
            self.endLag=datetime.timedelta(minutes=30)
        
        # Remove a few minutes from end time so we have all golden data
        self.end -= self.endLag
        
        if processorName:
            self.processor = processorName
        else:
            self.processor = type(self).__name__
        
        
        # Prepare a basic GA query object
        self.query = {
            'viewId': self.gaView,
            'includeEmptyRows': emptyRows,
            'hideTotals': True,
            # 'hideValueRanges': True,
            'pageSize': 100000,
            'samplingLevel': 'LARGE',
            'metrics': [{'expression': 'ga:uniqueEvents'}],
        }

        
        # Create a queue that contains DataFrames ready to be written to DB
        self.dbWriteQueue = queue.Queue()

        
            
            
    def getGA(self):
        # Create an object to call Google Analytics
        
        if self.ga is None:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentialsFile,
                ['https://www.googleapis.com/auth/analytics.readonly']
            )

            # Build the service object.
            self.ga = build('analyticsreporting', 'v4', credentials=credentials)
            
            
            # Management API is here: https://stackoverflow.com/questions/43050514/google-analytics-api-service-object-no-management-attribute
            self.gaManagement = build('analytics', 'v3', credentials=credentials)
        
        return self.ga



    def getGAViewObject(self):
            self.gaViewObject = self.gaManagement.management().profiles().get(
              accountId=self.gaAccount,
              webPropertyId=self.gaProperty,
              profileId=self.gaView
            ).execute()

#             self.logger.debug("View object: {}".format(json.dumps(self.gaViewObject)))
            
            return self.gaViewObject



        
            
            
    def getReportKeys(self, asDict=False):
        dimensionItemsList=[]

        for i in range(len(self.dimensions)):
            if 'key' in self.dimensions[i] and self.dimensions[i]['key']:
                if asDict:
                    dimensionItemsList.append({'title': self.dimensions[i]['title']})
                else:
                    dimensionItemsList.append(self.dimensions[i]['title'])
        
        return dimensionItemsList

    
    
    
    def getFinalReportColumns(self,onlykeys=False):
        dimensionItemsList=[]

        for i in range(len(self.dimensions)):
            if not onlykeys or (onlykeys and 'key' in self.dimensions[i] and self.dimensions[i]['key']):
                if 'transformspawncolumns' in self.dimensions[i] and self.dimensions[i]['transformspawncolumns']:
                    dimensionItemsList.extend(self.dimensions[i]['transformspawncolumns'])
                else:
                    dimensionItemsList.append(self.dimensions[i]['title'])

        return dimensionItemsList

    
    def finalReport(self):
        return self.report[self.getFinalReportColumns()]
    

    def dimensionItemsToList(self, item='name', asDict=False, filter=None):
        # Dimensions are passed to this object as this list:
        # [
        #     {
        #         'title': 'column_name',
        #         'name': 'ga:dateHourMinute',
        #         'type': 'datetime',
        #         'synccursor': True
        #     },
        #     {
        #         'title': 'other_column',
        #         'name': 'ga:dimension18',
        #         'type': 'int'
        #     },
        #     ...
        # ]
        
        dimensionItemsList=[]
        
        for i in range(len(self.dimensions)):
            if filter and i not in filter:
                # This is not a desired dimension. Skip.
                continue
                
            if item in self.dimensions[i]:
                if asDict:
                    dimensionItemsList.append({item: self.dimensions[i][item]})
                else:
                    dimensionItemsList.append(self.dimensions[i][item])
            else:
                dimensionItemsList.append(None)
        
        return dimensionItemsList


    
    
    def dimensionItemsToOrderBys(self):
        """
        Return of this method should go into query['orderBys']
        """

        dimensionItemsList=[]
        
        for i in range(len(self.dimensions)):
            if 'sort' in self.dimensions[i] and self.dimensions[i]['sort']:
                dimensionItemsList.append({
                    'fieldName': self.dimensions[i]['name'],
                    'sortOrder': 'ASCENDING'
                })
        
        if len(dimensionItemsList)>0:
            return dimensionItemsList
        else:
            return None


    def filterTimeStartToEnd(self):
        """
        Return of this method should go into query['dimensionFilterClauses']
        """
        
        dimensionItemsList=[]
        
        for i in range(len(self.dimensions)):
            if 'synccursor' in self.dimensions[i] and self.dimensions[i]['synccursor']:
                dimensionItemsList.append({
                    "operator": "AND",
                    "filters": [
                        {
                            "dimensionName": self.dimensions[i]['name'],
                            "operator": "NUMERIC_GREATER_THAN",
                            "expressions": [self.effectiveStart.strftime('%Y%m%d%H%M')]
                        },
                        {
                            "dimensionName": self.dimensions[i]['name'],
                            "operator": "NUMERIC_LESS_THAN",
                            "expressions": [self.end.strftime('%Y%m%d%H%M')]
                        }
                    ]
                })

        if len(dimensionItemsList)>0:
            return dimensionItemsList
        else:
            return None

        

    
    
    
    def subreportDimensions(self):
        """
        GA API supports up to 9 columns in a report.
        This method will split your dimensions in groups of 9, each part will contain
        the dimensions which have key=True plus several other dimensions, and API will
        be called for each subreport.

        Later those subreports will be joined togheter using the key columns to form the final report.
        
        This method returns a list of partitions. Each partition contains the index of the dimension to include.
        Something like this:
        [
            [0,1,   2,3,4,5,6,7,8],
            [0,1,   9,10,11],
        ]
        
        So you initialy you wanted 12 dimensions with dimension 0 and 1 having key=True. Thats why they apprear in both partitions.
        """
        GAMaxReportSize = 9 # Google Analytics API limit for number of dimensions
        subreportAdditionalDimensions = 1
        
        subreports=[]
        subreportKeys=[]
        
        for i in range(len(self.dimensions)):
            if 'key' in self.dimensions[i] and self.dimensions[i]['key']:
                subreportKeys.append(i)
                
        maxSize=min(GAMaxReportSize,subreportAdditionalDimensions+len(subreportKeys))

        subreport=0
        subreports.append(copy.deepcopy(subreportKeys))
        
        for i in range(len(self.dimensions)):
            # Iterate over each dimension and decide in which subreport to put it
            
            if 'key' in self.dimensions[i] and self.dimensions[i]['key']:
                # Skip key dimension because we already have them on subreportKeys
                continue
            
            if len(subreports[subreport]) >= maxSize:
                # If current subreport is too big, spawn a new one
                subreport += 1
                subreports.append(copy.deepcopy(subreportKeys))

            subreports[subreport].append(i)
            
        return subreports
    

    
    
    
    
    def getDateRangePartitions(self):
        periods=list(pd.period_range(start=self.effectiveStart, end=self.end, freq=f'{self.dateRangePartitionSize}d'))
        
        ranges=[]
        
        for p in periods:
            ranges.append(
                [
                    p.start_time.to_pydatetime(),
                    p.end_time.to_pydatetime()
                ]
            )
        
        return ranges    
    

    
    
    
    
    def callGA(self, body):
        # Some quota contol
        
        if 'lastStart' not in self.quotaControl:
            self.quotaControl['lastStart'] = datetime.datetime.now()
            self.quotaControl['count'] = 0
        
        if (self.apiQuota > 0) and (self.quotaControl['count'] > self.apiQuota):
            # What time it will be 100 seconds after lastStart?
            wait = (self.quotaControl['lastStart'] + datetime.timedelta(seconds=100)) - datetime.datetime.now()
            
            if wait.total_seconds()>0:
                # Wait until then
                self.logger.debug("Wait {}s, until {}, to avoid GA quota limits.".format(
                        wait.total_seconds(),
                        self.quotaControl['lastStart']+wait
                    )
                )
                time.sleep(wait.total_seconds())
            
            self.quotaControl['lastStart'] = datetime.datetime.now()
            self.quotaControl['count'] = 0
            
        
        # Some debug messages
        
        self.logger.debug("GA call count since {}: {}".format(self.quotaControl['lastStart'], self.quotaControl['count']))
        self.logger.debug("Query for GA: {}".format(json.dumps(body)))
        
        
        
        # Finaly call Google Analytics API
        
        try:
            report = self.ga.reports().batchGet(body=body, quotaUser=self.processor).execute()
        except Exception as e:
            self.logger.error("Unrecoverable error while calling GA: " + e)

        
        self.quotaControl['count']+=1

        return report



    
    
    
    def getReportData(self):
        """
        This is the core method. Algorithm:
        
        1. Prepare a GA object to talk to
        2. Prepare list of subreports (that will be joined later)
        3. Prepare list of time partitions to cover all period requested by user
        4. Iterate over list of subreports to get data
            4.1. For each subreport, iterate over list of time partitions
                4.1.1 For each subreport and time partiton, iterate over list of returned data pages
        5. Join subreports on key columns
        6. Perform simple data conversion for optimization (mostly string to datetime or to number)
        7. Perform more advanced data conversion with custom functions
        8. Calculate unique ID for each row
        
        """
        
        subreports = self.subreportDimensions()
        timepartitions = self.getDateRangePartitions()

        self.logger.debug(f'Subreport indexes: {subreports}')
        self.logger.debug(f'Time partitions to cover entire period requested: {timepartitions}')

        self.subreports = []

        for p in timepartitions:
            try:
                # Delete the previous query (if defined)
                del query
            except NameError:
                pass            
            
            query=copy.deepcopy(self.query)

            query['dateRanges'] = [{
                'startDate': p[0].date().isoformat(),
                'endDate':   p[1].date().isoformat()
            }]
            
            # For debugging:
            timePartitionName = '[{}]➔[{}]'.format(p[0].date().isoformat(),p[1].date().isoformat())
            
            
            # Fine tune time range as passed to object's `start` and `end` parameters
            timeLimits = self.filterTimeStartToEnd()
            if timeLimits:
                query['dimensionFilterClauses'] = timeLimits

            
            
            # Set order as passed by Dimensions structure
            order = self.dimensionItemsToOrderBys()
            if order:
                query['orderBys'] = order



            for i in range(len(subreports)):
                # For one time partition, iterate over all possible reports that consists of
                # key dimension with one additional dimension (a.k.a. subreport)
                
                query['dimensions'] = self.dimensionItemsToList(item='name', asDict=True, filter=subreports[i])

                
                # For debugging:
                dimTitles = self.dimensionItemsToList(item='title', asDict=False, filter=subreports[i])

                # Store report data here:
                result=[]
            
                if 'pageToken' in query: del query['pageToken']

                
                pageiteration=0
                nextPageToken=None
                sampling=None

                cont = True       # will be recalculated after each iteration

                while cont:
                    # Iterate over subreport pages of about 100000 rows
                    
                    self.logger.debug("Working on subreport {} of {}: 【{}】 【{}】 【{}】…".format(i+1,len(subreports),timePartitionName,dimTitles,pageiteration))
                    
                    if pageiteration>0:
                        query['pageToken'] = nextPageToken

                    try:
                        # Free big objects in RAM
                        del report
                    except NameError:
                        pass
                        
                    report = self.callGA(
                        body={
                            'reportRequests': [query],
                            'useResourceQuotas': True
                        }
                    )

                    if 'rowCount' in report['reports'][0]['data']:
                        # If report has data
                    
                        rowCount=report['reports'][0]['data']['rowCount']

                        nextPageToken=None
                        samplesReadCount=None
                        samplingSpaceSize=None

                        if 'nextPageToken' in report['reports'][0]:
                            nextPageToken=report['reports'][0]['nextPageToken']

                        if 'samplesReadCounts' in report['reports'][0]['data']:
                            samplesReadCount=int(report['reports'][0]['data']['samplesReadCounts'][0])

                        if 'samplingSpaceSizes' in report['reports'][0]['data']:
                            samplingSpaceSize=int(report['reports'][0]['data']['samplingSpaceSizes'][0])



                        for r in report['reports'][0]['data']['rows']:
                            result.append(r['dimensions'])

                        pageiteration += 1

                        self.logger.debug("Subreport page size has {} rows.".format(len(report['reports'][0]['data']['rows'])))

                        if samplesReadCount:
                            self.logger.debug("Sample space size: {}. Samples read: {}. Read {}% of sample space.".format(samplingSpaceSize,samplesReadCount,100*samplesReadCount/samplingSpaceSize))
                        else:
                            self.logger.debug("Data is complete and not sampled !")

                        self.logger.debug("Token for next page: {}.".format(nextPageToken))
                    else:
                        self.logger.debug("Dimension has no data for this time partition.")
                        
                    cont = (nextPageToken is not None)
                    
                    # At this point, a single page of a subreport was read containing 100.000 rows max. Continue to next page of same subreport.

                    
                # Even if there's no data (len(result)==0), I need an empty dataframe with all columns in the right place to later join them correctly.
                self.subreports.append(pd.DataFrame(
                    columns=self.dimensionItemsToList('title', filter=subreports[i]),
                    data=result
                ))
                self.logger.debug("Subreport shape size is {}×{}".format(
                    self.subreports[-1].shape[0],
                    self.subreports[-1].shape[1])
                )

                # Calculate a wanna-be-unique hash for each line based on key columns/dimensions
                keys=self.getReportKeys()
                self.subreports[-1] = GAAPItoDB.makePrimaryKey(self.subreports[-1],keys)


#                 buffer = io.StringIO()
#                 self.subreports[-1].info(verbose=True, buf=buffer)
#                 self.logger.debug("Subreport memory profile:\n{}".format(buffer.getvalue()))
                
                # Free some RAM
                del result
                

                # At this point all pages of a subreport inside a time partition were read and stored in a subreport DataFrame.
                # Continue to next subreport for same time partition.

                
            # At this point, all pages of all subreports inside a single time partition were read.
            # Now join and process data and set it ready to store in the database.
        
            if len(self.subreports) > 0:
                
                self.logger.debug("Joining {} subreports...".format(len(self.subreports)))
                
                self.report=self.subreports[0]

                # Start from second report family
                for i in range(1, len(self.subreports)):
                    # Join 2 reports by index, which is calculated as a hash from all reports common columns.
                    self.report=self.report.join(
                                other=self.subreports[i],
                                how='outer',
                                rsuffix=f"__{i}",
                                sort=False
                    )

                    # Coalesce values of key columns so the non-“__{i}” ones will have the data
                    for k in keys:
                        self.report[k]=self.report[k].combine_first(self.report[f'{k}__{i}'])
                    
                    # Delete overlapping columns
                    cols=self.report.columns
                    todrop=[]
                    for c in cols:
                        if f"__{i}" in c:
                            todrop.append(c)
                    self.report.drop(todrop, axis=1, inplace=True)
#                     del cols, todrop, c
                    
                    # Delete dataframe that was already joined and merged into self.report
                    destroyer=self.subreports[i]
                    self.subreports[i]=None
                    del destroyer
                    
                    # Force garbage collector
                    gc.collect()
                    
                    buffer = io.StringIO()
                    self.report.info(verbose=True, buf=buffer)
                    self.logger.debug("Report memory profile so far:\n{}".format(buffer.getvalue()))
                    




                # First Stage data conversion - operate over columns

                self.logger.debug("Optimizing data on {} dimensions...".format(len(self.dimensions)))
                for d in self.dimensions:
                    if 'type' in d:
                        orgname=d['title']
                        if 'keeporiginal' in d:
                            # Keep original data in a new column with suffix "__org"
                            orgname=d['title'] + "__org"
                            self.report[orgname]=self.report[d['title']]

                        if d['type'] == 'int':
                            self.report[d['title']]=pd.to_numeric(self.report[orgname],errors='raise')
                        if d['type'] == 'datetime':
                            # Convert to date and time
                            self.report[d['title']]=pd.to_datetime(self.report[orgname])
                            
                            # Add GA View's time zone just to convert time to UTC right away
                            self.report[d['title']]=self.report[d['title']].apply(lambda x: x.tz_localize(self.gaTimezone).tz_convert(None))



                if self.report.shape[0]>0:
                    # Second Stage data conversion - operate over entire dataframe
                    for d in self.dimensions:
                        if 'transform' in d:
                            self.logger.debug(f"Doing more complex data transformations for {d['title']}...")

                            # There is a second stage transformation declared for column.
                            # Call custom function with parameters
                            self.report = d['transform'](self.report,d)



                    # Calculate unique IDs for rows
                    self.logger.debug("Generate wanna-be unique IDs for rows...")
                    self.report = GAAPItoDB.makePrimaryKey(self.report, self.getFinalReportColumns(onlykeys=True))

                    # At this point we have a complete report for a time partition.
                    # Add it to the database writer queue.

                    self.logger.debug(f"Dispatching report of size {self.report.shape[0]}×{self.report.shape[1]} for DB writting...")
                    self.dbWriteQueue.put((timePartitionName, self.report))


                # Clean the way for more data
                self.report = None
#                 del self.subreports[:]
                self.subreports.clear()
            
        
        # At this point, there is no more time partitions to process. Thats the end of the work.
        
        self.logger.debug("Sending end of work signal for DB writer")
        self.dbWriteQueue.put(None) # Tell DB writting thread that's the end of work.

        



    def makePrimaryKey(df,listOfKeys):
        subreportPrimaryKeyName = '__row_id'
        
        # Initialize column
        df[subreportPrimaryKeyName]=""
        
        for k in listOfKeys:
            df[subreportPrimaryKeyName] += df[k].astype(str)

        df[subreportPrimaryKeyName] = df[subreportPrimaryKeyName].apply(GAAPItoDB.makeID)
        
        df.set_index(subreportPrimaryKeyName, inplace=True)
        
        return df




    def makeID(text):
        # Calculate a Shake 256 hash of 8 bytes for the text argument, use its hexdigest (textual) version
        
        idCalc=hashlib.new('shake_256')
        idCalc.update(text.encode('UTF-8'))
        return idCalc.hexdigest(8)


    def connectDB(self):
        # DB Connect
        self.logger.debug(f"Connecting to DB {self.dbURL}")

        try:
            self.db=sqlalchemy.create_engine(self.dbURL, encoding='utf8')
        except sqlalchemy.exc.SQLAlchemyError as error:
            self.logger.error('Can’t connect to DB.', exc_info=True)
            raise error

    
    def effectiveStartDate(self):
        """
        Calculate the effective start date to get report data according to this logic:
        
        If `incremental` is True, use start date as the last date and time that appears in the `targetTable`, column of dimension set with `synccursor`.
        
        If `restart` is True, ignore `incremental` and use `start` date.
        """
        
        if self.restart:
            # Ignore lastest data in database and start over
            self.effectiveStart=self.start
            self.lastSync = None
        else:
            self.effectiveStart = self.start   # set a default

            if self.incremental:
                # Effective start date is the last date of the `synccursor` column in DB, so find its value.
                timeColName=None

                # Find the column name that contains the date/time information
                for d in self.dimensions:
                    if 'synccursor' in d and d['synccursor']:
                        timeColName=d['title']
                        break

                # Check if table exists and get the top date value
                if self.db.dialect.has_table(self.db, self.targetTable):
                    lastSyncUTC = pd.read_sql(f"SELECT max(`{timeColName}`) as `{timeColName}` FROM {self.targetTable};", self.db)
                    if lastSyncUTC[timeColName][0] is not None:
                        # Time on DB is always UTC, so declare it as UTC, then convert to GA View's timezone and use it as time of last record.
                        self.effectiveStart = lastSyncUTC[timeColName][0].tz_localize('UTC').tz_convert(self.gaTimezone).to_pydatetime()

        return self.effectiveStart

    
    
    def writeDB(self, rawReport):
        report=rawReport[self.getFinalReportColumns()]
        
        
        # For debug purposes, get name of column used as sync parameter
        for d in self.dimensions:
            if 'synccursor' in d and d['synccursor']:
                timeColName=d['title']
                break

        if report.shape[0]==0:
            self.logger.debug('Report has no data, nothing to write.')
            return


            
        if self.restart:
            ifexists='replace'
            self.restart = False # Switch to false so we keep appending in next time partition
        else:
            ifexists='append'
        
        if self.update:
            # Use pandas.to_sql() to add final report lines to the database table
            report.reset_index().to_sql(
                self.targetTable,
                if_exists=ifexists,
                index=False,
                con=self.db
            )
            
            self.logger.debug('Wrote ({}) {} datapoints of {} columns, ranging from {} to {}'.format(
                    ifexists,
                    report.shape[0],
                    report.shape[1],
                    report[timeColName].min(),
                    report[timeColName].max()
                )
            )
        else:
            self.logger.warning('Didn’t write ({}) {} datapoints of {} columns, ranging from {} to {}'.format(
                    ifexists,
                    report.shape[0],
                    report.shape[1],
                    report[timeColName].min(),
                    report[timeColName].max()
                )
            )


            
    def databaseWriter(self):
        while True:
            dataToWrite = self.dbWriteQueue.get()
            
            if dataToWrite is None:
                self.logger.debug('End of DB writing thread from a None item.')
                self.dbWriteQueue.task_done()
                break
                
            timePartitionName = dataToWrite[0]
            rawReport = dataToWrite[1]
            
            self.logger.debug('Thread that writes data for {}'.format(timePartitionName))
            
            self.writeDB(rawReport)
            
            # Free some memory
            del rawReport
            
            self.dbWriteQueue.task_done()
            
        self.logger.debug('End of DB writing thread.')

                
            
    

    def sync(self):
        self.connectDB()
        self.effectiveStartDate()   # Sets self.effectiveStart
        
        # Create thread to write DataFrames to DB
        self.writer = threading.Thread(target=self.databaseWriter)
        self.writer.start() # start the thread
        
        # Start talking to GA and get report data
        self.getReportData()

        # Block until there is nothing on the queue
        if not self.dbWriteQueue.empty():
            self.dbWriteQueue.join()

