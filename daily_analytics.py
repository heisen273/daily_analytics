import os
import datetime
import argparse
import os
import time
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

os.chdir(os.path.dirname(os.path.realpath(__file__)))

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = './client_secrets.json' # Path to client_secrets.json file.

global appname
global analytics

def initialize_analyticsreporting():
  """Initializes the analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('./analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
  return analytics


def get_report(analytics):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        { 'filtersExpression': 'ga:sessions>0',
          'pageSize': 10000,
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': 'yesterday', 'endDate': 'yesterday'}],
          'dimensions': [{'name':'ga:pagePathLevel1'},{'name':'ga:date'}],
          'metrics': [{'expression': 'ga:sessions'}]
        }]
      }
  ).execute()
def get_report_iteration(analytics,nextPageToken):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        { 'filtersExpression': 'ga:sessions>0',
          'pageToken':nextPageToken,
          'pageSize': 10000,
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': 'yesterday', 'endDate': 'yesterday'}],
          'dimensions': [{'name':'ga:pagePathLevel1'},{'name':'ga:date'}],
          'metrics': [{'expression': 'ga:sessions'}]
        }]
      }
  ).execute()


def print_response(response,appname):
  appname = appname.replace('/','_')
  
  file = open('./SaveExtract/'+appname+'.csv','w')
  file.write('PagePathLevel1;Date;Sessions\n')
  print('processing '+appname+' . . . ')
  for report in response.get('reports',[]):
      columnHeader = report.get('columnHeader', {})
      dimensionHeaders = columnHeader.get('dimensions', [])
      metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
      rows = report.get('data', {}).get('rows', [])
      print(report.get('nextPageToken'))
      for row in rows:
        lst = []
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])
        for header, dimension in zip(dimensionHeaders, dimensions):
          lst.append(dimension)

        for i, values in enumerate(dateRangeValues):
          for metricHeader, value in zip(metricHeaders, values.get('values')):
            lst.append(value)
          lst[1] = datetime.datetime.strptime(lst[1], '%Y%m%d').strftime('%Y-%m-%d 00:00:00')#lst[1]+' 00:00:00'
          file.write(';'.join(lst).encode('utf-8')+'\n')
  while report.get('nextPageToken') is not None:
    try:
        analytics = initialize_analyticsreporting()
        response = get_report_iteration(analytics,report.get('nextPageToken'))
        for report in response.get('reports', []):


          columnHeader = report.get('columnHeader', {})
          dimensionHeaders = columnHeader.get('dimensions', [])
          metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
          rows = report.get('data', {}).get('rows', [])
          print('while',report.get('nextPageToken'))
          for row in rows:
            lst = []
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
              lst.append(dimension)

            for i, values in enumerate(dateRangeValues):
              for metricHeader, value in zip(metricHeaders, values.get('values')):
                lst.append(value)
              lst[1] = datetime.datetime.strptime(lst[1], '%Y%m%d').strftime('%Y-%m-%d 00:00:00')
              file.write(';'.join(lst).encode('utf-8')+'\n')

    except:
      time.sleep(15)
  print(appname+' processed')
  file.close()

def main():
  analytics = initialize_analyticsreporting()
  response = get_report(analytics)
  print_response(response,appname)

if __name__ == '__main__':
  #yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

  #VIEW_ID = '122698944'
  print('cleaning old reports. . .')
  os.system('rm -rf ./SaveExtract/*')
  views_lst = []
  storage = file.Storage('/home/erowz/analytics_Script/analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())
  analytics = build('analytics', 'v3', http=http)
  account_summaries = analytics.management().accountSummaries().list().execute()
  for item in account_summaries['items'][0]['webProperties']:
    #print item['name']

    if  item['name'] == 'QA site-annonce.fr' or item['name'] == 'QA RealEstate-UK' or item['name'] == 'International Search' or item['name'] == 're.bcdotnet.com' or item['name']== 'erowz.com':
      continue
    else:
      #print item['name']

      for element in item['profiles']:
        views_lst.append([element['name'].encode('utf-8'),element['id']])
  for view in views_lst:

    VIEW_ID=view[1]
    appname=view[0]
    if appname == 'MobileApp':
                pass
    else:
                main() 
