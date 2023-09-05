import sys, os, csv, re
import datetime as dt
import numpy as np
import pandas as pd
import geopandas as gpd
import configparser

sys.path.insert(0, os.path.join(os.path.realpath(os.path.dirname(__file__)), '..', '..'))
from tnc_annual_reports import reader as rr
from tnc_annual_reports import utils
from tnc_annual_reports.static.config_utils import string_to_list
from tnc_annual_reports.static import df_utils

def get_metrics(path, year, company, data_dict=None,
                completed_trips=True, incompleted_requests=True, 
                total_requests=True, vmt=True, driver_days=True, driver_hours=True,
                fix_names=True, chunksize=5000000, logfile=None):
                     
    reader = rr.ReportReader(indir=path, 
                             company=company, 
                             year=year,
                             data_dict=data_dict)
    
    #if logfile!=None:
    #    f = open(logfile, 'w')
    metrics = {} # metric -> report -> value
    
    #for included, name in zip([completed_trips, incompleted_requests, total_requests, vmt,
    #                           driver_days, driver_hours],
    #                          ['completed_trips', 'incompleted_requests', 'total_requests', 'vmt',
    #                           'driver_days', 'driver_hours']):
    #    if included:
    #        metrics[name] = {}
    
    if completed_trips:
        metrics['completed_trips'] = {}
        metrics['completed_trips']['Requests Accepted'] = 0
        metrics['completed_trips']['Aggregated Requests Accepted'] = 0
    if incompleted_requests:
        metrics['incompleted_requests'] = {}
        metrics['incompleted_requests']['Requests Not Accepted'] = 0
        metrics['incompleted_requests']['Aggregated Requests Not Accepte'] = 0
    if total_requests:
        metrics['total_requests'] = {}
        metrics['total_requests']['Disaggregate'] = 0
        metrics['total_requests']['Aggregate by zip code'] = 0
        metrics['total_requests']['Aggregate by month'] = 0
    if vmt:
        metrics['vmt'] = {}
        metrics['vmt']['Requests Accepted'] = 0
        metrics['vmt']['Number of Miles'] = 0
    if driver_days:
        metrics['driver_days'] = {}
        metrics['driver_days']['Number of Miles'] = 0
        metrics['driver_days']['Number of Hours'] = 0
    if driver_hours:
        metrics['driver_hours'] = {}
        metrics['driver_hours']['Requests Accepted'] = 0
        metrics['driver_hours']['Number of Hours'] = 0
            
    if completed_trips==True or total_requests==True or vmt==True:
        report = 'Requests Accepted'
        dh_redactions, vmt_redactions, records = 0, 0, 0
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
            metric = 'completed_trips'
            records += len(chunk)
            metrics[metric][report] += len(chunk)
            if vmt==True:
                metric = 'vmt'
                print(company, report, metric, records)
                for c in ['PeriodOneMilesTraveled','PeriodTwoMilesTraveled','PeriodThreeMilesTraveled']:
                    if c not in chunk.columns.tolist():
                        vmt_redactions += len(chunk)
                    else:
                        vmt_redactions += (chunk[c].astype(str).eq('Redacted') * 1).sum()
                try:
                    if vmt_redactions > 0:
                        #print('Reacted {}%'.format(100.0*vmt_redactions/(3*records)))
                        metrics[metric][report] = 'Reacted {}%'.format(100.0*vmt_redactions/(3*records))
                    else:
                        metrics[metric][report] += chunk[['PeriodOneMilesTraveled','PeriodTwoMilesTraveled','PeriodThreeMilesTraveled']].sum().sum()
                except:
                    print('failure')
                    metrics[metric][report] = np.nan
            if driver_hours==True:
                metric = 'driver_hours'
                for c in ['ReqAcceptedDate','PassengerPickupDate','PassengerDropoffDate']:
                    if c not in chunk.columns.tolist():
                        dh_redactions += len(chunk)
                    else:
                        dh_redactions += (chunk[c].eq('Redacted') * 1).sum()
                    
                if dh_redactions > 0:
                    #print('Reacted {}%'.format(100.0*dh_redactions/(3*records)))
                    metrics[metric][report] = 'Redacted {}%'.format(100.0*dh_redactions/(3*records))
                else:
                    chunk['p2_time'] = pd.to_datetime(chunk['ReqAcceptedDate'])
                    chunk['p3_time'] = pd.to_datetime(chunk['PassengerPickupDate'])
                    chunk['p4_time'] = pd.to_datetime(chunk['PassengerDropoffDate'])
                    
                    metrics[metric][report] += (chunk['p4_time'] - chunk['p3_time']).map(lambda x: x.total_seconds() / 3600.0).sum()
        report = 'Aggregated Requests Accepted'
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
            metric = 'completed_trips'
            metrics[metric][report] += chunk['TotalAcceptedTrips'].sum()
    if incompleted_requests==True or total_requests==True:
        report, metric = 'Requests Not Accepted', 'incompleted_requests'
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
            metrics[metric][report] += len(chunk)
        report = 'Aggregated Requests Not Accepte'
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
            metrics[metric][report] += chunk['TotalNotAcceptedTrips'].sum()
    if total_requests==True:
        metrics['total_requests']['Disaggregate'] = metrics['completed_trips']['Requests Accepted'] + metrics['incompleted_requests']['Requests Not Accepted']
        metrics['total_requests']['Aggregate by zip code'] = metrics['completed_trips']['Aggregated Requests Accepted'] + metrics['incompleted_requests']['Aggregated Requests Not Accepte']
        
        for report in ['Accessibility Report','Accessibility Report (Conf)', None]:
            if report in reader.tnames:
                break
        if report == None:
            raise Exception('no valid accessibility report name available')
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
            metrics['total_requests']['Aggregate by month'] += chunk['NumRidesReq'].sum()
    if driver_days==True or driver_hours==True:
        report = 'Number of Miles'
        
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
            metric = 'driver_days'
            metrics[metric][report] += len(chunk)
            if vmt:
                metric='vmt'
                metrics[metric][report] += chunk['DriverMilesRecordedDay'].sum()
        report = 'Number of Hours'
        for chunk in reader.read_report('Number of Hours', fix_names=fix_names, chunksize=chunksize):
            if 'DriverMilesRecordedDay' in chunk.columns.tolist():
                chunk.rename(columns={'DriverMilesRecordedDay':'DriverHoursRecordedDay'}, inplace=True)
            metric = 'driver_days'
            metrics[metric][report] += len(chunk)
            if driver_hours:
                metric='driver_hours'
                metrics[metric][report] += chunk['DriverHoursRecordedDay'].sum()
    return metrics
    
def metric_dict_to_df(metrics):
    data = []
    for company, d1 in metrics.items():
        for metric, d2 in d1.items():
            for report, value in d2.items():
                data.append([company,metric,report,value])
                
    return pd.DataFrame(data, columns=['company','metric','report','value'])
    
def title_case(s):
    s = s.strip()
    return s[0].upper() + s[1:].lower()
    
def get_table_name(company, metric):
    parts = metric.split('_')
    table_name = title_case(company)
    for p in parts:
        table_name = table_name + '_' + title_case(p)
    return table_name + ' by Report'
    
def metric_dict_to_summary_df(metrics, company, metric):
    m = metrics[company][metric]
    
    if metric == 'completed_trips':
        index=['Disaggregate trip list\n(from Requests Accepted)',
               'Aggregate by zip code\n(from Aggregated Requests Accepted)']
        columns=['Completed Trips']
        data = [m['Requests Accepted'], m['Aggregated Requests Accepted']]
    elif metric == 'total_requests':
        index=['Disaggregate trip list\n(from Requests Accepted, Requests Not Accepted)',
               'Aggregate by zip code\n(from Aggregated Requests Accepted, Aggregated Requests Not Accepted)',
               'Aggregate by month\n(from Accessibility Report)']
        columns=['Trip Requests']
        data = [m['Disaggregate'], m['Aggregate by zip code'], m['Aggregate by month']]
    elif metric == 'incompleted_requests':
        index=['Disaggregate trip list\n(from Requests Not Accepted)','Aggregate by zip code\n(from Aggregated Requests Not Accepted)']
        columns = ['Incompleted Trip Requests']
        data = [m['Requests Not Accepted'], m['Aggregated Requests Not Accepte']]
    elif metric == 'vmt':
        index=['Disaggregate by trip list\n(from Requests Accepted)', 'Aggregate by driver day\n(from Number of Miles)']
        columns=['VMT']
        data = [m['Requests Accepted'], m['Number of Miles']]
    elif metric == 'driver_days':
        index=['Aggregate by driver day\n(from Number of Miles)', 'Aggregate by driver day\n(from Number of Hours)']
        columns=['Driver Days']
        data = [m['Number of Miles'], m['Number of Hours']]
    elif metric == 'driver_hours':
        index=['Disaggregate trip list\n(from Requests Accepted)', 'Aggregate by driver day\n(from Number of Hours)']
        columns=['Driver Hours']
        data=[m['Requests Accepted'], m['Number of Hours']]
    else:
        raise Exception('unknown metric {}'.format(metric))

    df = pd.DataFrame(data, index=index, columns=columns)
    df['Difference'] = np.nan
    df['Pct Difference'] = np.nan
    
    if isinstance(df.iloc[0,0], str):
        df['Difference'] = 'Unknown'
        df['Pct Difference'] = 'Unknown'
    else:
        df['Difference'] = df[columns[0]] - df.iloc[0,0]
        df['Pct Difference'] = df['Difference'] / df.iloc[0,0]
        df.iloc[0, 1] = '-'
        df.iloc[0, 2] = '-'
    return df
    
    
if __name__=='__main__':
    args = sys.argv[1:]
    configpath = args[0]
    combined = {}
    
    cp = configparser.ConfigParser()
    f = open(configpath, 'r')
    cp.read_file(f)
    
    indir = cp.get('consistency','root')
    outdir = cp.get('consistency','outdir')
    ddpath = cp.get('consistency','data_dict')
    year = cp.getint('consistency','year')
    ofile = cp.get('consistency','summary_ofile')
    
    completed_trips = cp.getboolean('consistency','completed_trips')
    incompleted_requests = cp.getboolean('consistency','incompleted_requests')
    total_requests = cp.getboolean('consistency','total_requests')
    vmt = cp.getboolean('consistency','vmt')
    driver_days = cp.getboolean('consistency','driver_days')
    driver_hours = cp.getboolean('consistency','driver_hours')
    
    companies = string_to_list(cp.get('consistency','companies'))
    
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        
    dd = utils.read_data_dictionary(path=ddpath, sheet_names=None)
    desc = utils.get_report_descriptions(path=ddpath, sheet_names=None)
    
    for company in companies:
        print('getting metrics for {}'.format(company))
        path = r'{}\{}\{}'.format(indir,year,company)
        metrics = get_metrics(path, year, company, dd, 
                              completed_trips, incompleted_requests, total_requests, 
                              vmt, driver_days, driver_hours,
                              fix_names=True,
                              )
        combined[company] = metrics
        print('done with metrics for {}'.format(company))
    
    #df = metric_dict_to_df(metrics)
    writer = pd.ExcelWriter(os.path.join(outdir,ofile))
    row, col = 0, 0
    
    for company, d1 in combined.items():
        print(company)
        for metric, d2 in d1.items():
            print(metric)
            df = metric_dict_to_summary_df(combined, company, metric)
            table_name = get_table_name(company, metric)
            
            row, col = df_utils.df_to_excel(writer, 
                                            sheet_name='Consistency',
                                            table_name=table_name,
                                            df=df,
                                            startrow=row,
                                            startcol=col,
                                            row_buffer=2,
                                            offset_rows=True,
                                            index=True)
        row, col = 0, 5
        
    writer.save()
    
    