#!/usr/bin/env python3

from . import GAEmailToDB
import logging.handlers
import logging
import argparse
import configparser

        
def prepareLogging(level=logging.INFO):
    # Switch between INFO/DEBUG while running in production/developping:
    logging.getLogger('GAEmailToDB').setLevel(level)
    logging.getLogger('GAEmailToDB.GAEmailToDB').setLevel(level)
    logging.captureWarnings(True)
    
    if level == logging.DEBUG:
        # Show messages on screen too:
        logging.getLogger().addHandler(logging.StreamHandler())

    # Send messages to Syslog:
    #logging.getLogger().addHandler(logging.handlers.SysLogHandler(address='/dev/log'))
    logging.getLogger().addHandler(logging.handlers.SysLogHandler())




def prepareArgs():
    parser = argparse.ArgumentParser(description='EXTRACT Google Analytics custom-report CSVs attached on e-mail messages on a certain IMAP folder, TRANSFORM them into meaningfull table and LOAD results into a database')
    
    parser.add_argument('--imap_server', dest='imap_server',
                        help='IMAP server hostname as imap.gmail.com')

    parser.add_argument('--imap_folder', dest='imap_folder',
                        help='IMAP folder to process unread messages from')

    parser.add_argument('--imap_subject', dest='imap_subject',
                        help='Messages subject to filter and read')

    parser.add_argument('--imap_user', dest='imap_user',
                        help='User to log into IMAP server')

    parser.add_argument('--imap_password', dest='imap_password',
                        help="User's password to log into IMAP server")

    parser.add_argument('--database', '--db', dest='database_url',
                        help='Destination database URL as «mysql://user:pass@host.com/dbname?charset=utf8mb4»')
    
    parser.add_argument('--updatedb', '-u', dest='database_update', default=True, action='store_false',
                        help='Get updates from IMAP but do not update database')    

    parser.add_argument('--debug', '-d', dest='debug', default=False, action='store_true',
                        help='Be more verbose and output messages to console in addition to (the default) syslog')

    parser.add_argument('--config', '-c', dest='config', default='sources.conf',
                        help='Config file with IMAP credentials and destination database URL')

    args=parser.parse_args()
    
    if args.config is None:
    	if (args.imap_server is None or
				args.imap_folder is None or
				args.imap_user is None or
				args.imap_password is None or
				args.database_url is None):
    		parser.error("Either pass all parameter or a config file with --config.")
    
    config={'imap': {}, 'database': {}}
    
    for k in vars(args):
        a=k.split('_')
        if getattr(args,k) is not None:
            if len(a)==1:
                config[a[0]]=getattr(args,k)
            else:
                config[a[0]][a[1]]=getattr(args,k)
                
#     print(f'Command line config: {config}')
            
    return config
    
    



def main():
    args=prepareArgs()

    # Read config file
    context=configparser.ConfigParser()
    if 'config' in args:
        context.read(args['config'])
        context=context._sections


    # Overwrite config file variables with the ones passed on command line
    context['debug']=args['debug']
    for s in ['database', 'imap']:
        for v in args[s].keys():
#             print(f"Walking on: args[{s}][{v}]={args[s][v]}")
            context[s][v]=args[s][v]

    
#     print(args)
#     print(context)
    
    # Merge configuration file parameters with command line arguments
#     context.update(args)
    
    # print(context)
    
    # Setup logging
    if context['debug']:
        prepareLogging(logging.DEBUG)
    else:
        prepareLogging()
        
        

    # Prepare syncing machine
    etl = GAEmailToDB(config=context)
    
    
    # Read Typeform updates and write to DB
    etl.sync()
    





if __name__ == "__main__":
    main()

