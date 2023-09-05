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
    
def describe_reports(path, year, company, reports=None, data_dict=None, nrows=None):
    
    reader = rr.ReportReader(indir=path, 
                             company=company, 
                             year=year)
                             
    # create a dict for the reports (rp) as name -> None (placeholder for report)
    rp = {}
    if reports == None:
        reports = reader.tnames
    
    for tname in reports:
        if not data_dict==None:
            template = data_dict[tname].copy()
            report = template.copy()
            report['included'] = False
            
            new_cols = [col.strip() for col in report.columns]
            report.columns = new_cols
            expected_fields = len(report)
        else:
            report = reader.read_report(tname, nrows=0).T
            
        records = 0
        right_only = []
        approx_matches = set()
        nulls = None
        redacts = None
        
        
        for chunk in reader.read_report(tname, chunksize=5000000, nrows=nrows):
            print(tname, records)
            rename = {}
            new_cols = [col.strip().replace(r'ï»¿','') for col in chunk.columns]
            chunk.columns = new_cols
            
            for idx, row in report.iterrows():
                m, match_type = utils.find_name_match(idx, chunk.columns.tolist())
                if match_type == 'approx': approx_matches.add(idx)
                if m != None:
                    rename[m] = idx
                    report.loc[idx, 'included'] = True
                    report.loc[idx, 'matched_field'] = m
                    report.loc[idx, 'match_type'] = 'exact' # approx matches are updated later.
            for col in chunk.columns:
                if col not in rename.keys():
                    right_only.append(col)
            chunk.rename(columns=rename, inplace=True)
            
            #chunk.index = chunk.index + records # increment the index
            records = records + len(chunk)

            # Count null/missing/not provided values by field
            tmp = pd.DataFrame(data=(chunk.isnull() * 1 + chunk.eq('Not provided') * 1).sum(), 
                               columns=['missing'])
            if isinstance(nulls, pd.DataFrame):
                nulls = nulls + tmp
            else:
                nulls = tmp
            
            # Count redacted values
            tmp = pd.DataFrame(data=(chunk.eq('Redacted') * 1).sum(), 
                               columns=['redacted'])
            if isinstance(redacts, pd.DataFrame):
                redacts = redacts + tmp
            else:
                redacts = tmp
        report.loc[list(approx_matches), 'match_type'] = 'approx'
        if records == 0:
            report['included'] = False
            report['matched_field'] = None
            report['match_type'] = None
            report['missing'] = records
            report['redacted'] = records
            report['dtype_match'] = False
            report['total_records'] = records
        else:
            nulls.index.name = 'Field'
            report = pd.merge(report, nulls, left_index=True, right_index=True, how='left')
            redacts.index.name = 'Field'
            report = pd.merge(report, redacts, left_index=True, right_index=True, how='left')
            report['dtype_match'] = True
            report['total_records'] = records
        
        missing_fields = report.loc[report['included'].eq(False),].index.tolist()
        dtype_mismatches = []

        for idx, row in report.iterrows():
            if idx in missing_fields:
                continue

        rp[tname] = report
        
    return rp

if __name__=='__main__':
    args = sys.argv[1:]
    configpath = args[0]
    combined = {}
    
    cp = configparser.ConfigParser()
    f = open(configpath, 'r')
    cp.read_file(f)
    
    indir = cp.get('inventory','root')
    outdir = cp.get('inventory','outdir')
    ddpath = cp.get('inventory','data_dict')
    year = cp.getint('inventory','year')
    ofile = cp.get('inventory','summary_ofile')
    
    companies = string_to_list(cp.get('inventory','companies'))
    
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        
    dd = utils.read_data_dictionary(path=ddpath, sheet_names=None)
    desc = utils.get_report_descriptions(path=ddpath, sheet_names=None)
    
    for company in companies:
        path = r'{}\{}\{}'.format(indir,year,company)
        rp = describe_reports(path, year, company, data_dict=dd)
        combined[company] = rp
        
    #print(combined[company].keys())
    writer = pd.ExcelWriter(os.path.join(outdir,ofile), engine='xlsxwriter')
    completeness = {}
    
    summary = {'fields':{},'cells':{}}
    summary['fields']['total'] = pd.DataFrame(index=dd.keys(), columns=companies)
    summary['fields']['complete'] = pd.DataFrame(index=dd.keys(), columns=companies)
    summary['cells']['total'] = pd.DataFrame(index=dd.keys(), columns=companies)
    summary['cells']['missing'] = pd.DataFrame(index=dd.keys(), columns=companies)
    summary['cells']['redacted'] = pd.DataFrame(index=dd.keys(), columns=companies)
    summary['cells']['complete'] = pd.DataFrame(index=dd.keys(), columns=companies)
    
    for tname in dd.keys():
        row, col = 0, 0
        r = None
        df2 = pd.DataFrame(index=companies,columns=['missing','redacted','complete','total_records'])
        
        for company in companies:
            df = combined[company][tname].reset_index()
            df['included'] = df['included'].map(lambda x: 'Yes' if x==True else 'No') + df['match_type'].map(lambda x: '*' if x=='approx' else '')
            #df['complete'] = df['total_records'] - (df['missing']+df['redacted'])
            df['complete'] = df['total_records'] - (df['redacted'])
            
            df2.loc[company, 'total_records'] = df['total_records'].sum()
            for c in ['missing','redacted','complete']:
                df['pct_{}'.format(c)] = df[c] / df['total_records']
                df2.loc[company, c] = df[c].sum()
                df2.loc[company, 'pct_{}'.format(c)] = df2.loc[company, c] / df2.loc[company, 'total_records']
            
            df = df[['Field','Confidential or Public','Mandatory or Optional','Field Description','included','total_records','missing','redacted','complete',
                     'pct_missing','pct_redacted','pct_complete']]

            # any fields that are not 100% redacted, and are required
            public = df.loc[df['Confidential or Public'].eq('Public') & df['Mandatory or Optional'].eq('Mandatory')]
            summary['fields']['total'].loc[tname, company] = len(public)
            summary['fields']['complete'].loc[tname, company] = (public['pct_redacted'].eq(0)).sum()
            
            # percentage of total cells that are not redacted
            summary['cells']['total'].loc[tname, company] = public['total_records'].sum()
            summary['cells']['missing'].loc[tname, company] = public['missing'].sum()
            summary['cells']['redacted'].loc[tname, company] = public['redacted'].sum()
            summary['cells']['complete'].loc[tname, company] = public['complete'].sum()

            df.rename(columns={'included':'{} Included'.format(company),
                               'missing':'{} Missing'.format(company),
                               'redacted':'{} Redacted'.format(company),
                               'complete':'{} Complete'.format(company),
                               'total_records': '{} Total Records'.format(company),
                               'pct_missing':'{} Percent Missing'.format(company),
                               'pct_redacted':'{} Percent Redacted'.format(company),
                               'pct_complete':'{} Percent Complete'.format(company)},
                      inplace=True)
            
            
            
            if not isinstance(r, pd.DataFrame):
                r = df.copy()
            else:
                r = pd.merge(r, df)
                
        r = r[['Field','Confidential or Public','Mandatory or Optional','Field Description',
               'Uber Included','Uber Missing','Uber Redacted','Uber Complete', 
               'Uber Percent Missing','Uber Percent Redacted','Uber Percent Complete', 'Uber Total Records',
               'Lyft Included',
               'Lyft Included','Lyft Missing','Lyft Redacted','Lyft Complete', 
               'Lyft Percent Missing','Lyft Percent Redacted','Lyft Percent Complete', 'Lyft Total Records']]

        df2.loc['total',:] = df2.sum()
        for c in ['missing','redacted','complete']:
            df2.loc['total', 'pct_{}'.format(c)] = df2.loc['total', c] / df2.loc['total', 'total_records']
                
        row, col = df_utils.df_to_excel(writer, 
                             sheet_name=tname,
                             table_name='{} Completeness Summary'.format(tname),
                             df=r,
                             startrow=row,
                             startcol=col,
                             row_buffer=2,
                             offset_rows=True,
                             reset_cols=True,
                             index=False)
        
        row, col = df_utils.df_to_excel(writer, 
                             sheet_name=tname,
                             table_name = 'Cell Completeness Summary',
                             df=df2,
                             startrow=row,
                             startcol=col,
                             offset_rows=True,
                             reset_cols=True,
                             )
    # write the topsheet tables
    row, col = 0, 0 
    
    summary['fields']['total'].loc['Total',:] = summary['fields']['total'].sum()
    summary['fields']['complete'].loc['Total',:] = summary['fields']['complete'].sum()
    summary_table_fields = summary['fields']['complete'].divide(summary['fields']['total'])

    summary['cells']['total'].loc['Total',:] = summary['cells']['total'].sum()
    summary['cells']['missing'].loc['Total',:] = summary['cells']['missing'].sum()
    summary['cells']['redacted'].loc['Total',:] = summary['cells']['redacted'].sum()
    summary['cells']['complete'].loc['Total',:] = summary['cells']['complete'].sum()
    summary_table_cells = summary['cells']['complete'].divide(summary['cells']['total'])
    
    print(summary['fields']['total'])
    print(summary['fields']['complete'])
    print(summary_table_fields)
    
    print(summary['cells']['total'])
    print(summary['cells']['complete'])
    print(summary_table_cells)
    
    
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Field Completeness',
                                    table_name = 'Field Completeness Summary',
                                    df=summary_table_fields,
                                    startrow=row,
                                    startcol=col,
                                    offset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Field Completeness',
                                    table_name = 'Field Completeness Summary (count of present and unredacted fields)',
                                    df=summary['fields']['complete'],
                                    startrow=row,
                                    startcol=col,
                                    offset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Field Completeness',
                                    table_name = 'Field Completeness Summary (total fields)',
                                    df=summary['fields']['total'],
                                    startrow=row,
                                    startcol=col,
                                    offset_rows=True,
                                    reset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Cell Completeness',
                                    table_name = 'Percent Complete (Present & Unredacted)',
                                    df=summary_table_cells,
                                    startrow=row,
                                    startcol=col,
                                    offset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Cell Completeness',
                                    table_name = 'Missing Records',
                                    df=summary['cells']['missing'],
                                    startrow=row,
                                    startcol=col,
                                    offset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Cell Completeness',
                                    table_name = 'Redacted Records',
                                    df=summary['cells']['redacted'],
                                    startrow=row,
                                    startcol=col,
                                    offset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Cell Completeness',
                                    table_name = 'Complete Records (Present & Unredacted)',
                                    df=summary['cells']['complete'],
                                    startrow=row,
                                    startcol=col,
                                    offset_cols=True,
                                    )
    row, col = df_utils.df_to_excel(writer, 
                                    sheet_name='Cell Completeness',
                                    table_name = 'Total Records',
                                    df=summary['cells']['total'],
                                    startrow=row,
                                    startcol=col,
                                    offset_rows=True,
                                    reset_cols=True,
                                    )
    writer.save()

    w = pd.ExcelWriter(os.path.join(outdir,'tmp.xlsx'), engine='xlsxwriter')
    df.to_excel(w, sheet_name='sheet', startrow=1,startcol=0)
    workbook = w.book
    title_fmt = workbook.add_format({'bold':True,'italic':True})
    firstrow_fmt = workbook.add_format({'bottom':5,'top':5,'bold':True})
    lastrow_fmt = workbook.add_format({'bottom':10})
    worksheet = w.sheets['sheet']
    worksheet.conditional_format(1,0,1,len(df.columns),
                                 {'type': 'cell', 'criteria':'>=', 'value':0,'format': firstrow_fmt})
    w.save()
    #w.close()