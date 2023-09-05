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


    #disagg_name = 'Completed Trips\n(from Requests Accepted)'
    #agg_name = 'Completed Trips\n(from Aggregated Requests Accepted)'

def get_requests_accepted_metrics(path, year, company, 
                                  completed_trips=True, vmt=True, 
                                  non_pool=True, pool_no_match=True, pool_match=True, 
                                  incomplete_pool=True, incomplete_non_pool=True,
                                  data_dict=None, fix_names=False,
                                  chunksize=5000000, nrows=None, logfile=None):
                     
    reader = rr.ReportReader(indir=path, 
                             company=company, 
                             year=year,
                             data_dict=data_dict)
    metrics = {}
    report = 'Requests Accepted'

    if completed_trips:
        metrics['completed_trips'] = 0
    if vmt:
        metrics['vmt'] = 0    
    if non_pool:
        metrics['non_pool'] = 0
    if pool_no_match:
        metrics['pool_no_match'] = 0
    if pool_match:
        metrics['pool_match'] = 0
    if incomplete_pool:
        metrics['incomplete_pool'] = 0
    if incomplete_non_pool:
        metrics['incomplete_non_pool'] = 0
        
    vmt_redactions, records = 0, 0
    req_name, match_name = None, None
    for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize, nrows=nrows):
        if req_name == None:
            for field_name in ['Pool Request', 'PoolRequest']:
                if field_name in chunk.columns.tolist():
                    req_name = field_name
            for field_name in ['Pool Match', 'PoolMatch']:
                if field_name in chunk.columns.tolist():
                    match_name = field_name
            
        records += len(chunk)
        print(records)
        if completed_trips:
            metrics['completed_trips'] += len(chunk)
        if non_pool:
            metrics['non_pool'] += len(chunk.loc[chunk[req_name].isin(['N','n'])])
        if pool_no_match:
            metrics['pool_no_match'] += len(chunk.loc[chunk[req_name].isin(['Y','y']) & chunk[match_name].isin(['N','n'])])
            #print(chunk.groupby(match_name).size())
        if pool_match:
            metrics['pool_match'] += len(chunk.loc[chunk[req_name].isin(['Y','y']) & chunk[match_name].isin(['Y','y'])])
        if vmt:
            for c in ['PeriodOneMilesTraveled','PeriodTwoMilesTraveled','PeriodThreeMilesTraveled']:
                if c not in chunk.columns.tolist():
                    vmt_redactions += len(chunk)
                else:
                    vmt_redactions += (chunk[c].astype(str).eq('Redacted') * 1).sum()
            try:
                if vmt_redactions > 0:
                    #print('Reacted {}%'.format(100.0*vmt_redactions/(3*records)))
                    metrics['vmt'] = 'Reacted {}%'.format(100.0*vmt_redactions/(3*records))
                else:
                    metrics['vmt'] += chunk[['PeriodOneMilesTraveled','PeriodTwoMilesTraveled','PeriodThreeMilesTraveled']].sum().sum()
            except:
                print('can\'t get vmt')
                metrics['vmt'] = np.nan
    
    if incomplete_pool or incomplete_non_pool:
        report = 'Requests Not Accepted'
        for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize, nrows=nrows):
            if req_name == None:
                for field_name in ['Pool Request', 'PoolRequest']:
                    if field_name in chunk.columns.tolist():
                        req_name = field_name
                for field_name in ['Pool Match', 'PoolMatch']:
                    if field_name in chunk.columns.tolist():
                        match_name = field_name
                
            records += len(chunk)
            print(records)
            if incomplete_pool:
                metrics['incomplete_pool'] += len(chunk.loc[chunk[req_name].isin(['Y','y'])])
            if incomplete_non_pool:
                metrics['incomplete_non_pool'] += len(chunk.loc[chunk[req_name].isin(['N','n'])])
    return metrics

def get_agg_requests_accepted_metrics(path, year, company, completed_trips=True, vmt=True, 
                                      data_dict=None, fix_names=False, logfile=None):
    reader = rr.ReportReader(indir=path, 
                             company=company, 
                             year=year,
                             data_dict=data_dict)
    metrics = {}
    if completed_trips:
        metrics['completed_trips'] = 0
        report = 'Aggregated Requests Accepted'
        for chunk in reader.read_report(report, fix_names=fix_names):
            metrics['completed_trips'] += chunk['TotalAcceptedTrips'].sum()
    if vmt:
        metrics['vmt'] = 0
        report = 'Number of Miles'
        for chunk in reader.read_report(report, fix_names=fix_names):
            metrics['vmt'] += chunk['DriverMilesRecordedDay'].sum()
    return metrics

def get_requests_by_zipcode(path, year, company, data_dict=None, fix_names=False, chunksize=5000000):
    reader = rr.ReportReader(indir=path, 
                             company=company, 
                             year=year,
                             data_dict=data_dict)

    trips = []
    for chunk in reader.read_report('Aggregated Requests Accepted', fix_names=fix_names, chunksize=chunksize):
        trips.append(chunk[['ZipCodeRequest','TotalAcceptedTrips']])
    trips = pd.concat(trips).rename(columns={'ZipCodeRequest':'zipcode','TotalAcceptedTrips':'completed_trips'})
    
    na_trips = []
    report = 'Aggregated Requests Not Accepte'
    for chunk in reader.read_report(report, fix_names=fix_names, chunksize=chunksize):
        na_trips.append(chunk[['ZipCodeRequest','TotalNotAcceptedTrips']])
    na_trips = pd.concat(na_trips).rename(columns={'ZipCodeRequest':'zipcode','TotalNotAcceptedTrips':'incompleted_requests'})
    
    trips['zipcode'] = trips['zipcode'].replace('Not captured',0).fillna(0).astype('int64')
    na_trips['zipcode'] = na_trips['zipcode'].replace('Not captured',0).fillna(0).astype('int64')
    
    df = pd.merge(trips, na_trips, on='zipcode', how='outer').fillna(0)
    df['total_requests'] = df['completed_trips'] + df['incompleted_requests']
    df['share_completed'] = df['completed_trips'] / df['total_requests']
    return df
    
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
    
def metric_dict_to_summary_df(metrics, metric):
    if metric == 'completed_trips':
        index=['Disaggregate trip list\n(from Requests Accepted)',
               'Aggregate by zip code\n(from Aggregated Requests Accepted)',
               ]
    elif metric == 'vmt':
        index=['Disaggregate by trip list\n(from Requests Accepted)', 
               'Aggregate by driver day\n(from Number of Miles)',
               ]
    else:
        raise Exception('unknown metric {}'.format(metric))

    columns = list(metrics.keys())
    data = []
    
    for report_type in ['disagg', 'agg']:
        row = []
        for company in columns:
            row.append(metrics[company][report_type][metric])
        data.append(row)
        
    df = pd.DataFrame(data, index=index, columns=columns)
    
    for col in columns:
        a, b =  df[col].iloc[0], df[col].iloc[1]
        
        try:
            df.loc['Difference', col] = b - a
            df.loc['Pct Difference', col] = df.loc['Difference',col] / a
            df.loc['Minimum',col] = min(a, b)
            df.loc['Maximum',col] = max(a, b)
        except:
            df.loc['Difference', col] = 'Unknown'
            df.loc['Pct Difference', col] = 'Unknown'
            df.loc['Minimum',col] = 'Unknown'
            df.loc['Maximum',col] = 'Unknown'
    for idx in index:
        try:
            df.loc[idx, 'Total'] = df.loc[idx, columns].sum(axis=1)
        except:
            df.loc[idx, 'Total'] = 'Unknown'
    return df
    
    
if __name__=='__main__':
    args = sys.argv[1:]
    configpath = args[0]
    combined = {}
    
    cp = configparser.ConfigParser()
    f = open(configpath, 'r')
    cp.read_file(f)
    
    indir = cp.get('characteristics','root')
    outdir = cp.get('characteristics','outdir')
    ddpath = cp.get('characteristics','data_dict')
    year = cp.getint('characteristics','year')
    ofile = cp.get('characteristics','summary_ofile')
    
    completed_trips = cp.getboolean('characteristics','completed_trips')
    incompleted_requests = cp.getboolean('characteristics','incompleted_requests')
    total_requests = cp.getboolean('characteristics','total_requests')
    non_pool = cp.getboolean('characteristics','non_pool')
    pool_no_match = cp.getboolean('characteristics','pool_no_match')
    pool_match = cp.getboolean('characteristics','pool_match')
    incomplete_pool = cp.getboolean('characteristics','incomplete_pool')
    incomplete_non_pool = cp.getboolean('characteristics','incomplete_non_pool')
    vmt = cp.getboolean('characteristics','vmt')
    companies = string_to_list(cp.get('characteristics','companies'))
    zipcode_to_county = cp.get('characteristics','zipcode_to_county')
    zipcode_ofile = cp.get('characteristics','zipcode_ofile')
    county_ofile = cp.get('characteristics','county_ofile')
    
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        
    dd = utils.read_data_dictionary(path=ddpath, sheet_names=None)
    desc = utils.get_report_descriptions(path=ddpath, sheet_names=None)
    
    z2c = pd.read_csv(zipcode_to_county)
    zipcode = None
    
    for company in companies:
        combined[company] = {}
        print('getting disaggregate metrics for {}'.format(company))
        path = r'{}\{}\{}'.format(indir,year,company)
        disagg = get_requests_accepted_metrics(path, year, company, 
                                               completed_trips, vmt, non_pool, pool_no_match, pool_match,
                                               incomplete_pool, incomplete_non_pool,
                                               data_dict=dd, fix_names=True)
        combined[company]['disagg'] = disagg
        print(disagg)
        print('getting aggregate metrics for {}'.format(company))
        agg = get_agg_requests_accepted_metrics(path, year, company, data_dict=dd, fix_names=True)
        combined[company]['agg'] = agg
        
        #print('getting vmt (driver-miles) metrics for {}'.format(company))
        #print('done with metrics for {}'.format(company))
        df = get_requests_by_zipcode(path, year, company, fix_names=True, data_dict=dd)        
        cols = df.columns.tolist()
        cols.remove('zipcode')
        rename = {f:'{}_{}'.format(company, f) for f in cols}
        df = df.rename(columns=rename)
        if not isinstance(zipcode, pd.DataFrame):
            zipcode = df.fillna(0)
        else:
            zipcode = pd.merge(zipcode, df, on='zipcode', how='outer').fillna(0)
    
    for i, c in zip([0,1,2],['completed_trips','incompleted_requests','total_requests']):
        zipcode.insert(i,c,0.0)
        for company in companies:
            zipcode[c] = zipcode[c] + zipcode['{}_{}'.format(company, c)]
    
    z2c['sqmi'] = z2c['ALAND20'] / (1609.34**2)
    z2c['county_sqmi'] = z2c['C_ALAND20'] / (1609.34**2)
    z2c = z2c[['ZCTA5CE20','COUNTYFP20','NAME20','sqmi','county_sqmi']].rename(columns={'ZCTA5CE20':'zipcode',
                                                                                        'COUNTYFP20':'county_fips',
                                                                                        'NAME20':'county'})
                                                                                         
    zipcode = pd.merge(z2c, zipcode, how='outer').fillna(0)
    county = zipcode.groupby(['county_fips','county','county_sqmi'], as_index=False).sum().rename({'sqmi':'zip_sqmi'})
    zipcode.drop(columns='county_sqmi', inplace=True)
    
    zipcode.to_csv(os.path.join(outdir,zipcode_ofile), index=False)
    county.to_csv(os.path.join(outdir,county_ofile), index=False)
    writer = pd.ExcelWriter(os.path.join(outdir,ofile))
    row, col = 0, 0
    
    # topline
    for metric in ['completed_trips','vmt']:
        print(metric)
        df = metric_dict_to_summary_df(combined, metric)
        
        row, col = df_utils.df_to_excel(writer, 
                                        sheet_name='Topsheet',
                                        table_name='{} by Company'.format(metric),
                                        df=df,
                                        startrow=row,
                                        startcol=col,
                                        row_buffer=2,
                                        offset_rows=True,
                                        index=True)
    
    # county trips
    county_fips_index = [37, 75, 73, 59, 1, 85, 81, 67, 65, 13]
    df = county.set_index('county_fips').loc[county_fips_index,
                                             ['county','completed_trips',
                                              'incompleted_requests','total_requests','county_sqmi']]
    df.loc[100,'county'] = 'All Other Counties'
    df.loc[101,'county'] = 'Total'
    df.loc[102,'county'] = 'Non-SF'
    
    for m in ['completed_trips','incompleted_requests','total_requests','county_sqmi']:
        df.loc[101,m] = county[m].sum()
        df.loc[100,m] = df.loc[101,m] - df.loc[county_fips_index,m].sum()
        df.loc[102,m] = df.loc[101,m] - df.loc[75,m]
    
    df['share_completed'] = df['completed_trips'] / df['total_requests']
    df['trips_per_sqmi'] = df['completed_trips'] / df['county_sqmi']
    
    row, col = 0, 0
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='County',
                                    table_name='Trips by County',
                                    df=df,
                                    startrow=row,
                                    startcol=col,
                                    row_buffer=2,
                                    offset_rows=True,
                                    index=True)
    # pooling
    data = []
    for m in ['incomplete_non_pool','incomplete_pool','non_pool','pool_no_match','pool_match']:
        row = []
        for c in ['Uber','Lyft']:
            row.append(combined[c]['disagg'][m])
        data.append(row)
    df = pd.DataFrame(data=data, index=['incomplete_non_pool','incomplete_pool','non_pool','pool_no_match','pool_match'], columns=['Uber','Lyft'])
    row, col = 0, 0
    row, col = df_utils.df_to_excel(writer, 
                                sheet_name='Pooling',
                                table_name='Pooling by Company',
                                df=df,
                                startrow=row,
                                startcol=col,
                                row_buffer=2,
                                offset_rows=True,
                                index=True)    
    writer.save()
    
    