#!/usr/bin/env python3

import logging
import datetime
import dateutil.parser
import copy
import email
from email.message import EmailMessage
import pandas as pd
import io
import imaplib
import hashlib
import sqlalchemy


module_logger = logging.getLogger(__name__)

class GACustomReportEmailToDB(object):
    def __init__(
                        self,
                        processorName=None,
                        
                        config=None,
                
                        imap_server=None,
                        imap_folder=None,
                        imap_subject=None,
                        imap_user=None,
                        imap_password=None,
                        
                        imap_sent_since=None,
                        imap_sent_before=None,
            
                        database_url=None,
                        database_update=True,
            
                        reportColumns=None,
                        tableFamilyAndUniqueColumn=None,
                        columnsRenameMap=None,
                        subReportIDColumns=None,
                        reportIDColumns=None
        ):
            
        # Setup logging
        if __name__ == '__main__':
            self.logger=logging.getLogger('{a}.{b}'.format(a=type(self).__name__, b=type(self).__name__))
        else:
            self.logger=logging.getLogger('{a}.{b}'.format(a=__name__, b=type(self).__name__))

        
        time=[imap_sent_since,imap_sent_before]
        
        for t in range(len(time)):
            if not isinstance(time[t], (datetime.datetime, datetime.date)):
                if type(time[t]) == str:
                    time[t]=dateutil.parser.isoparse(time[t])
        
        self.config={}
        
        # Process configuration
        if config is not None:
            self.config=copy.deepcopy(config)
            
            self.config['imap']['sent_since']=time[0]
            self.config['imap']['sent_before']=time[1]
        else:
            self.config['database']={
                'url': database_url,
                'update': database_update
            }
            
            self.config['imap']={
                'server': imap_server,
                'folder': imap_folder,
                'subject': imap_subject,
                'user': imap_user,
                'password': imap_password,
                'sent_since': time[0],
                'sent_before': time[1]
            }

        
        self.logger.debug(f"Configuration is: {self.config}")

        self.tableFamilyAndUniqueColumn=tableFamilyAndUniqueColumn
        self.columnsRenameMap=columnsRenameMap
        self.subReportIDColumns=subReportIDColumns
        self.reportColumns=reportColumns
        self.reportIDColumns=reportIDColumns
        self.reportColumns=reportColumns
        
        if processorName:
            self.processor = processorName
        else:
            self.processor = type(self).__name__
        
        self.dataFromMail=None




    def sync(self):
        reports=self.readMailAttachedFiles()
        self.consolidateReports(reports)
        self.writeDB()



    def writeDB(self):    
        if self.dataFromMail is None:
            self.logger.warning('DataFrame is empty')
            return
            
        # Conecta no DB do arquivo de configuração
        self.logger.debug(f"Connecting to DB {self.config['database']['url']}")

        try:
            db=sqlalchemy.create_engine(self.config['database']['url'], encoding='utf8')
        except sqlalchemy.exc.SQLAlchemyError as error:
            self.logger.error('Can’t connect to DB.', exc_info=True)
            raise error


        # Usa o Pandas to_sql() para adicionar as linhas do dataframe na tabela
        if self.config['database']['update']:
            self.dataFromMail.reset_index().to_sql(
                self.config['imap']['subject'],
                index=False,
                if_exists='append',
                con=db
            )
            self.logger.debug(f'Wrote {self.dataFromMail.shape[0]} datapoints of {self.dataFromMail.shape[1]} columns')
        else:
            self.logger.debug(f'Didn’t write {self.dataFromMail.shape[0]} datapoints of {self.dataFromMail.shape[1]} columns')

        
        db.dispose()


    # Virtual functions for pre and post processing of a single attached CSV,
    # so children can redefine them
    def preprocessSingleReport(self, df):
        return df
        
    def postprocessSingleReport(self, df):
        return df



    def readMailAttachedFiles(self):
        self.logger.debug(f"{self.processor} is connecting to IMAP {self.config['imap']['user']}@{self.config['imap']['server']}/{self.config['imap']['folder']}")
        
        reports = {}        
        
        M = imaplib.IMAP4_SSL(self.config['imap']['server'])
        M.login(self.config['imap']['user'], self.config['imap']['password'])
        
        folderStatus = M.select(self.config['imap']['folder'])
                
        if folderStatus[0] == 'NO':
            self.logger.warning("Folder {} doesn't exist".format(self.config['imap']['folder']))
            return reports

        
        
        # https://www.atmail.com/blog/advanced-imap/
        # https://gist.github.com/martinrusev/6121028
        # https://www.example-code.com/python/imap_search.asp
        
        imapSearch=['UNSEEN']
#         imapSearch=[]
        
        if self.config['imap']['subject']:
            imapSearch.append('SUBJECT "{}"'.format(self.config['imap']['subject']))
        
        if self.config['imap']['sent_since']:
            imapSearch.append('SENTSINCE {}'.format(self.config['imap']['sent_since'].strftime("%d-%b-%Y")))
        
        if self.config['imap']['sent_before']:
            imapSearch.append('SENTBEFORE {}'.format(self.config['imap']['sent_before'].strftime("%d-%b-%Y")))
        
        
        self.logger.debug("Criteria for mail fetch → {}".format(" ".join(imapSearch)))

        
        # Result for IMAP search will be something like:
        #     UNSEEN SUBJECT "ga_parcelas_atrasadas_clicadas" SENTSINCE 01-Mar-2019 SENTBEFORE 21-Mar-2020 
        typ, data = M.search(None, " ".join(imapSearch))
        
        
        attachmentContent=None
        inter=None

        for num in data[0].split():
            typ, data = M.fetch(num, '(RFC822)')
        
            for response_part in data:
                # Processa cada e-mail
            
                if isinstance(response_part, tuple):
                    original = email.message_from_bytes(response_part[1])
                    
                    self.logger.debug(f"Reading e-mail “{original['Subject']}” from {original['Date']}")


#                   print(original['From'])
#                   print(original['Subject'])
#                   print(original['Date'])
#                   typ, data = mail.store(num,'+FLAGS','\\Seen')

                    date_str=original.get('date')
                    if date_str:
                        date_tuple=email.utils.parsedate_tz(date_str)
                        if date_tuple:
                            thedate=datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))

                    for part in original.walk():
                        # Processa cada anexo do e-mail/mensagem
                        d=None
                
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if part.get('Content-Disposition') is None:
                            continue

                        filename = part.get_filename()
                
                        try:
                            attachmentContent=io.BytesIO(part.get_payload(decode=True))
                            
                            inter=pd.read_csv(
                                attachmentContent,
                                encoding='UTF-8',
                                comment='#'
                            )
                            
                            # O inter contém o CSV já carregado num DataFrame.
                            # Processa e transforma para algo mais pronto para o DB:
                            
                            inter=self.preprocessSingleReport(inter)
                            t,d = self.transformSingleReport(inter)
                            d=self.postprocessSingleReport(d)
                            
                            d['mail_date']=thedate
                            
                            self.logger.debug(f"Processed CSV of e-mail {original['Date']}: {d.shape[0]} lines")


                            # Appenda o DataFrame processado a sua família de DataFrames
                    
                            if t in reports.keys():
                                reports[t]=reports[t].append(d)
                            else:
                                reports[t]=d

                        except pd.errors.EmptyDataError as e:
                            self.logger.warning(f"Got empty data on mail from {original['Date']}")



        # Adeus ao servidor IMAP:
        M.close()
        M.logout()
        
        return reports



    def consolidateReports(self,reports):
        # Junta (join) as inúmeras famílias de DataFrames encontrados:
        receivedReportFamilies = list(reports.keys())
        
        if len(receivedReportFamilies)<1:
            self.dataFromMail = None
            return
        
        self.dataFromMail = reports[receivedReportFamilies[0]]
        
        # Start from second report family
        for r in receivedReportFamilies[1:]:
            # Join 2 reports by index, which is calculated as a hash from all reports common columns.
            self.dataFromMail=self.dataFromMail.join(
                        other=reports[r],
                        how='outer',
                        rsuffix=r+"_",
                        sort=False
            )

        # Sign report
        self.dataFromMail['processor'] = self.processor

        # Calculate unique ID for each line     
        self.dataFromMail['uniqueid']=""
        
        for c in self.reportIDColumns:
            self.dataFromMail['uniqueid'] += self.dataFromMail[c].astype(str)
            
        self.dataFromMail['uniqueid'] = self.dataFromMail['uniqueid'].apply(GACustomReportEmailToDB.makeID)
        
        self.dataFromMail.set_index('uniqueid', inplace=True)
        
        self.dataFromMail=self.dataFromMail[self.reportColumns]

        self.logger.debug(f'Final DataFrame of size {self.dataFromMail.shape[0]}×{self.dataFromMail.shape[1]}')



    def makeID(text):
        # Calcula um hash Shake 256 de 5 caracteres para o texto no argumento
        idCalc=hashlib.new('shake_256')
        idCalc.update(text.encode('UTF-8'))
        return idCalc.hexdigest(8)



    def transformSingleReport(self, orgDF):
        idColumn='__report_id'
        df=orgDF.copy()

        # Rename columns
        df.rename(inplace=True, columns=self.columnsRenameMap)

        df[idColumn]=""
        for c in self.subReportIDColumns:
            df[idColumn] += df[c].astype(str)
            
        df[idColumn] = df[idColumn].apply(GACustomReportEmailToDB.makeID)

        df.set_index(idColumn, inplace=True)
    
        # Identify table family by some unique column
        for family in self.tableFamilyAndUniqueColumn.keys():
            if self.tableFamilyAndUniqueColumn[family] in df.columns:
                self.logger.debug(f'CSV matches sub-report family {self.processor}::{family}')
                return family, df
        
        Except("Didn't match a family of reports for CSV")



