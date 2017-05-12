import os
import datetime
import subprocess
os.chdir(os.path.dirname(os.path.realpath(__file__)))

files = [f for f in os.listdir('./SaveExtract/') if f[-3:] == 'csv'] #and f == 'www.for-sale.co.uk.csv']
print(files)
def bq_loader(f,table):
        s = subprocess.Popen(['bq','load','--skip_leading_rows=1','--max_bad_records=50','--allow_jagged_rows','--allow_quoted_newlines','--field_delimiter=;','LogFilesv2_Dataset.'+table, './SaveExtract/'+f], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout = s.stdout.readlines()
        if stdout == ['\n','\n']:
            print '\nSuccessfully loaded '+f,
            return
        if stdout[2].split(' ')[0].lower() == 'warning':
            print '\nSuccessfully loaded '+f,
            return
        if stdout[2].split(' ')[0].lower() == 'bigquery' :
            repeat = True
            print 'failed to load file, repeating 10 times until first success. . .'

for f in files:
    table_name = f.split('.csv')[0].lower()

    if 'gebraucht-kaufen.at' in table_name:
        os.system('bq mk LogFilesv2_Dataset.AT_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'AT_Visits')
        print 'AT_Visits'

    elif 'brasil' in table_name:
        os.system('bq mk LogFilesv2_Dataset.BR_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'BR_Visits')
        print 'BR_Visits'
    elif 'gebraucht-kaufen.de' in table_name:
        os.system('bq mk LogFilesv2_Dataset.DE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'DE_Visits')

        print 'DE_Visits'
    elif 'canada.for-sale.com' in table_name:
        os.system('bq mk LogFilesv2_Dataset.CA_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'CA_Visits')
        print 'CA_Visits'
    elif 'for-sale.in' in table_name:
        os.system('bq mk LogFilesv2_Dataset.IN_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'IN_Visits')
        print 'IN_Visits'
    elif 'www.for-sale.co.uk' in table_name:
        os.system('bq mk LogFilesv2_Dataset.UK_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'UK_Visits')
        print 'UK_Visits'
    elif 'immobilier-france.fr' in table_name:
        os.system('bq mk LogFilesv2_Dataset.FR_RE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'FR_RE_Visits')
        print 'FR_RE_Visits'
    elif 'homes.for-sale.co.uk' in table_name:
        os.system('bq mk LogFilesv2_Dataset.UK_RE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'UK_RE_Visits')
        print 'UK_RE_Visits'
    elif 'in-vendita.it' in table_name:
        os.system('bq mk LogFilesv2_Dataset.IT_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'IT_Visits')
        print 'IT_Visits'
    elif 'for-sale.ie' in table_name:
        os.system('bq mk LogFilesv2_Dataset.IE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'IE_Visits')
    elif 'compra-venta.es' in table_name:
        os.system('bq mk LogFilesv2_Dataset.ES_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'ES_Visits')
        print 'ES_Visits'
    elif 'birmingham' in table_name:
        os.system('bq mk LogFilesv2_Dataset.UK_Birmingham_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'UK_Birmingham_Visits')
        print 'UK_Birmingham_Visits'
    elif 'australia' in table_name:
        os.system('bq mk LogFilesv2_Dataset.AU_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'AU_Visits')
        print 'AU_Visits'
    elif 'za' in table_name:
        os.system('bq mk LogFilesv2_Dataset.ZA_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'ZA_Visits')
        print 'ZA_Visits'
    elif 'leeds' in table_name:
        os.system('bq mk LogFilesv2_Dataset.UK_Leeds_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'UK_Leeds_Visits')
        print 'UK_Leeds_Visits'
    elif 'nigeria' in table_name:
        os.system('bq mk LogFilesv2_Dataset.NG_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'NG_Visits')
        print 'NG_Visits'
    elif 'manchester' in table_name:
        os.system('bq mk LogFilesv2_Dataset.UK_Manchester_Visits URL:string,date:string,sessions:integer')

        bq_loader(f,'UK_Manchester_Visits')
        print 'UK_Manchester_Visits'
    elif 'london' in table_name:
        os.system('bq mk LogFilesv2_Dataset.UK_London_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'UK_London_Visits')
        print 'UK_London_Visits'
    elif 'site-annonce.be' in table_name:
        os.system('bq mk LogFilesv2_Dataset.BE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'BE_Visits')
        print 'BE_Visits'
    elif 'site-annonce.fr' in table_name:
        os.system('bq mk LogFilesv2_Dataset.FR_Visits URL:string,date:string,sessions:integer')

        bq_loader(f,'FR_Visits')
    elif 'te-koop' in table_name:
        os.system('bq mk LogFilesv2_Dataset.NL_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'NL_Visits')
        print 'NL_Visits'
    elif 'used.forsale' in table_name:
        os.system('bq mk LogFilesv2_Dataset.US_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'US_Visits')
        print 'US_Visits'
    elif 'venta.com.ar' in table_name:
        os.system('bq mk LogFilesv2_Dataset.AR_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'AR_Visits')
        print 'AR_Visits'
    elif 'venta.com.mx' in table_name:
        os.system('bq mk LogFilesv2_Dataset.MX_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'MX_Visits')
        print 'MX_Visits'
    elif 'chile' in table_name:
        os.system('bq mk LogFilesv2_Dataset.CL_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'CL_Visits')
        print 'CL_Visits'
    elif 'colombia' in table_name:
        os.system('bq mk LogFilesv2_Dataset.CO_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'CO_Visits')
        print 'CO_Visits'
    elif 'peru' in table_name:
        os.system('bq mk LogFilesv2_Dataset.PE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'PE_Visits')
        print 'PE_Visits'
    elif 'venezuela' in table_name:
        os.system('bq mk LogFilesv2_Dataset.VE_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'VE_Visits')
        print 'VE_Visits'
    elif 'uzywane' in table_name:
        os.system('bq mk LogFilesv2_Dataset.PL_Visits URL:string,date:string,sessions:integer')
        bq_loader(f,'PL_Visits')
        print 'PL_Visits'
