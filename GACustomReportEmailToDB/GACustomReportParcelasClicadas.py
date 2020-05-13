import pandas as pd
import logging
import copy
from . import GACustomReportEmailToDB


module_logger = logging.getLogger(__name__)





class GACustomReportParcelasClicadasV1(GACustomReportEmailToDB):
    tableFamilyAndUniqueColumn = {
        'evento': 'event'
    }
    
    subReportIDColumns=['date', 'sucursal', 'apolice']
    
    reportIDColumns=['date', 'sucursal', 'apolice', 'event']

    columnsRenameMap = {
        'Unique Events':                                 'count',
        'Event Label':                                   'event',
    }
            
    reportColumns=[
        'date', 'sucursal', 'apolice',
        'count', 'event',
        'processor', 'mail_date'
    ]
    
    def __init__(
                        self,
                        config=None,
                
                        imap_server=None,
                        imap_folder=None,
                        imap_subject=None,
                        imap_user=None,
                        imap_password=None,
                        
                        imap_sent_since=None,
                        imap_sent_before=None,                        
            
                        database_url=None,
                        database_update=True
        ):
            
        super().__init__(
            config=config,
                
            imap_server=imap_server,
            imap_folder=imap_folder,
            imap_subject=imap_subject,
            imap_user=imap_user,
            imap_password=imap_password,
                        
            database_url=database_url,
            database_update=database_update,
            
            imap_sent_since=imap_sent_since,
            imap_sent_before=imap_sent_before,

            reportColumns=self.reportColumns,
            tableFamilyAndUniqueColumn=self.tableFamilyAndUniqueColumn,
            columnsRenameMap=self.columnsRenameMap,
            subReportIDColumns=self.subReportIDColumns,
            reportIDColumns=self.reportIDColumns
        )


    def preprocessSingleReport(self, df):

        toAppend=[]

        # Find all rows with multiple data for this column ("123-12345,234-76543" etc)
        toSplit=df[df['Sucursal - Apólice'].str.contains(',')].copy()
        if toSplit.shape[0]>0:
            for i in toSplit.iterrows():
                r=dict(i[1])
                self.logger.debug(f"[{i[0]}] has multiple values: {r['Sucursal - Apólice']}")
                for o in r['Sucursal - Apólice'].split(','):
                    # Clone line but change value on column that has splits
                    nr=copy.deepcopy(r)
                    nr['Sucursal - Apólice']=o
                    toAppend.append(nr)

            # Delete those rows
            df.drop(toSplit.index,inplace=True)
        
            # Append cloned and splited rows
            df=df.append(toAppend)
        
            # Free some RAM
            del toSplit
            del toAppend


        # Split sucursal e apólice
        df['sucursal']=df['Sucursal - Apólice'].apply(
            lambda x: int(x.split(',')[0].split('-')[0])
        )
        
        df['apolice']=df['Sucursal - Apólice'].apply(
            lambda x: int(x.split(',')[0].split('-')[1])
        )


        # Corrige a data e hora
        df['date']=pd.to_datetime(
            df['Hour of Day'].astype(str)+df['Minute'].astype(str)+'30',
            format='%Y%m%d%H%M%S',
            utc=None,
            errors='ignore'
        )
        
        return df





class GACustomReportParcelasClicadasV2(GACustomReportParcelasClicadasV1):
    tableFamilyAndUniqueColumn = {
        'corretor1': 'corretor_cpd',
    }

    columnsRenameMap = {
        'Corretor - Código':                             'corretor_cpd',
        'Unique Events':                                 'count',
        'Event Label':                                   'event'
    }
            
    reportColumns=[
        'date', 'sucursal', 'apolice',
        'corretor_cpd',
        'count', 'event',
        'processor', 'mail_date'
    ]





class GACustomReportParcelasClicadasV3(GACustomReportParcelasClicadasV2):
    tableFamilyAndUniqueColumn = {
        'evento': 'event',
        'cliente': 'cliente_cpfcnpj_sha256'
    }

    columnsRenameMap = {
        'CPF | CNPJ - Sha256':                           'cliente_cpfcnpj_sha256',
        'Cliente - CPF | CNPJ - Sha256':                 'cliente_cpfcnpj_sha256',
        'Sucursal':                                      'corretor_sucursal',
        'Corretor - Código':                             'corretor_cpd',
        'Unique Events':                                 'count',
        'Event Label':                                   'event'
    }
    
    subReportIDColumns=['date','sucursal','apolice']
    
    reportIDColumns=['date', 'cliente_cpfcnpj_sha256', 'sucursal', 'apolice', 'event']
    
    reportColumns=[
        'date', 'sucursal', 'apolice',
        'cliente_cpfcnpj_sha256',
        'corretor_sucursal', 'corretor_cpd',
        'count', 'event',
        'processor', 'mail_date'
    ]



class GACustomReportParcelasClicadasV4(GACustomReportParcelasClicadasV3):
    tableFamilyAndUniqueColumn = {
        'evento': 'event',
        'corretor1': 'corretor_cpd',
        'corretor2': 'corretor_cnpj_sha256',
        'cliente': 'cliente_cpfcnpj_sha256'
    }

    columnsRenameMap = {
        'CPF | CNPJ - Sha256':                           'cliente_cpfcnpj_sha256',
        'Cliente - CPF | CNPJ - Sha256':                 'cliente_cpfcnpj_sha256',
        'Cliente - CPF | CNPJ - ZeroPad - Sha256':       'cliente_cpfcnpj_zeropad_sha256',
        'Sucursal':                                      'corretor_sucursal',
        'Corretor - Código':                             'corretor_cpd',
        'Corretor - Login - CNPJ - Sha256':              'corretor_cnpj_sha256',
        'Corretor - Login - CNPJ - ZeroPad - Sha256':    'corretor_cnpj_zeropad_sha256',
        'Unique Events':                                 'count',
        'Event Label':                                   'event'
    }
            
    reportColumns=[
        'date', 'sucursal', 'apolice',
        'cliente_cpfcnpj_sha256', 'cliente_cpfcnpj_zeropad_sha256',
        'corretor_sucursal', 'corretor_cpd',
        'corretor_cnpj_sha256', 'corretor_cnpj_zeropad_sha256',
        'count', 'event',
        'processor', 'mail_date'
    ]







