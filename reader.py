# 2020
import sys, os, re
import pandas as pd

from .utils import read_data_dictionary, find_name_match, fix_field_names
    
YEAR_COMPANY_TNAME_FNAME = {2020:
                                {'uber':{'Driver Names & IDs':'Driver_Names_and_Identifier',
                                        'Accessibility Report (Conf)':'Accessibility_Report_Confidential', #conf/public for 2020 only
                                        'Accessibility Report (Public)':'Accessibility_Report_Public', #conf/public for 2020 only
                                        'Accessibility Complaints (Conf)':'Accessibility_Complaints_Confidential', #conf/public for 2020 only
                                        'Accessibility Complaints (Pub)':'Accessibility_Complaints_Public', #conf/public for 2020 only
                                        'Accidents & Incidents':'Accidents_And_Incidents',
                                        'Assaults & Harassments':'Assaults_And_Harassments',
                                        '50,000+ Miles':'Driver_50000_Miles',
                                        'Number of Hours':'Driver_Number_Of_Hours',
                                        'Number of Miles':'Driver_Number_Of_Miles',
                                        'Driver Training':'Driver_Training',
                                        'Law Enforcement Citations':'Law_Enforcement_Citations',
                                        'Off-platform Solicitation':'Off_Platform_Solicitations',
                                        'Aggregated Requests Accepted':'Ride_Requests_Accepted_Aggregate', 
                                        'Requests Accepted':'Ride_Requests_Accepted', 
                                        #'Requests Accepted Periods':'Rides_Requests_Accepted_Periods', # 2021 only
                                        'Aggregated Requests Not Accepte':'Ride_Requests_Not_Accepted_Aggregated', 
                                        'Requests Not Accepted':'Ride_Requests_Not_Accepted', 
                                        'Suspended Drivers':'Suspended_Drivers',
                                        'Total Violations & Incidents':'Total_Violations',
                                        'Zero Tolerance':'Zero_Tolerance',
                                        },
                                'lyft':{'Driver Names & IDs':'Driver_Names_and_Identifier',
                                        'Accessibility Report (Conf)':'Accessibility_Report_Confidential', #conf/public for 2020 only
                                        'Accessibility Report (Public)':'Accessibility_Report_Public', #conf/public for 2020 only
                                        'Accessibility Complaints (Conf)':'Accessibility_Complaints_Confidential', #conf/public for 2020 only
                                        'Accessibility Complaints (Pub)':'Accessibility_Complaints_Public', #conf/public for 2020 only
                                        'Accidents & Incidents':'Accidents_And_Incidents',
                                        'Assaults & Harassments':'Assaults_And_Harassments',
                                        '50,000+ Miles':'Driver_50000_Miles',
                                        'Number of Hours':'Driver_Number_Of_Hours',
                                        'Number of Miles':'Driver_Number_Of_Miles',
                                        'Driver Training':'Driver_Training',
                                        'Law Enforcement Citations':'Law_Enforcement_Citations',
                                        'Off-platform Solicitation':'Off_Platform_Solicitations',
                                        'Aggregated Requests Accepted':'Rides_Requests_Accepted_Aggregated', # lyft misspelled report name, s/b "ride" not "rides"; "aggregated" instead of "aggregate"
                                        'Requests Accepted':'Rides_Requests_Accepted', # lyft misspelled report name, s/b "ride" not "rides"
                                        #'Requests Accepted Periods':'Rides_Requests_Accepted_Periods', # 2021 only
                                        'Aggregated Requests Not Accepte':'Rides_Requests_Not_Accepted_Aggregated', # lyft misspelled report name, s/b "ride" not "rides"
                                        'Requests Not Accepted':'Rides_Requests_Not_Accepted', # lyft misspelled report name, s/b "ride" not "rides"
                                        'Suspended Drivers':'Suspended_Drivers',
                                        'Total Violations & Incidents':'Total_Violations',
                                        'Zero Tolerance':'Zero_Tolerance',
                                        }
                                },
                            2021: 
                                {'uber':{'Driver Names & IDs':'Driver_Names_and_Identifier_Public',
                                        'Accessibility Report':'Accessibilty_Report_Public',  # report name is misspelled
                                        'Accessibility Complaints':'Accessibilty_Complaints_Public', # report name is misspelled
                                        'Accidents & Incidents':'Accidents_And_Incidents_Public',
                                        'Assaults & Harassments':'Assaults_And_Harassments_Public',
                                        '50,000+ Miles':'Driver_50000_Miles_Public',
                                        'Number of Hours':'Driver_Number_Of_Hours_Public',
                                        'Number of Miles':'Driver_Number_Of_Miles_Public',
                                        'Driver Training':'Driver_Training_Public',
                                        'Law Enforcement Citations':'Law_Enforcement_Citations_Public',
                                        'Off-platform Solicitation':'Off_Platform_Solicitations_Public',
                                        'Aggregated Requests Accepted':'Rides_Requests_Accepted_Aggregate_Public', 
                                        'Requests Accepted':'Rides_Requests_Accepted_Public', 
                                        'Requests Accepted Periods':'Rides_Requests_Accepted_Periods_Public', 
                                        'Aggregated Requests Not Accepte':'Rides_Requests_Not_Accepted_Aggregated_Public', 
                                        'Requests Not Accepted':'Rides_Requests_Not_Accepted_Public', 
                                        'Suspended Drivers':'Suspended_Drivers_Public',
                                        'Total Violations':'Total_Violations_Public',
                                        'Zero Tolerance':'Zero_Tolerance_Public',
                                        },
                                'lyft':{'Driver Names & IDs':'Driver_Names_and_Identifier_Public',
                                        'Accessibility Report':'Accessibility_Report_Public', 
                                        'Accessibility Complaints':'Accessibility_Complaints_Public', 
                                        'Accidents & Incidents':'Accidents_And_Incidents_Public',
                                        'Assaults & Harassments':'Assaults_And_Harassments_Public',
                                        '50,000+ Miles':'Driver_50000_Miles_Public',
                                        'Number of Hours':'Driver_Number_Of_Hours_Public',
                                        'Number of Miles':'Driver_Number_Of_Miles_Public',
                                        'Driver Training':'Driver_Training_Public',
                                        'Law Enforcement Citations':'Law_Enforcement_Citations_Public',
                                        'Off-platform Solicitation':'Off_Platform_Solicitations_Public',
                                        'Aggregated Requests Accepted':'Rides_Requests_Accepted_Aggregate_Public', 
                                        'Requests Accepted':'Rides_Requests_Accepted_Public', 
                                        'Requests Accepted Periods':'Rides_Requests_Accepted_Periods_Public', 
                                        'Aggregated Requests Not Accepte':'Rides_Requests_Not_Accepted_Aggregated_Public', 
                                        'Requests Not Accepted':'Rides_Requests_Not_Accepted_Public', 
                                        'Suspended Drivers':'Suspended_Drivers_Public',
                                        'Total Violations':'Total_Violations_Public',
                                        'Zero Tolerance':'Zero_Tolerance_Public',
                                        }
                                }
                        }

class ReportReader:
    def __init__(self, indir='', company='uber', year=2020, data_dict=None):
        self.indir = indir
        self.company = company.lower()
        self.year = year
        
        self.tname_to_fname = YEAR_COMPANY_TNAME_FNAME[self.year][self.company]
        self.tnames = self.tname_to_fname.keys()
        self.fname_to_tname = {v: k for k, v in self.tname_to_fname.items()}
        self.fnames = self.fname_to_tname.keys()
        
        if isinstance(data_dict, str):
            self.data_dict = utils.read_data_dictionary(data_dict)
        elif isinstance(data_dict, dict):
            self.data_dict = data_dict
        elif data_dict==None:
            self.data_dict = None
        else:
            raise Exception('Unknown data dict type {}.  Must be string path to data dictionary or a dictionary of pandas DataFrames.'.format(type(data_dict)))
            
        
    def get_report_file_attrs(self, f):
        c = re.compile('(?P<resub>Resubmission_){0,1}(?P<cc>[a-zA-Z0-9]+)_(?P<date>\d{8}|\d{4}_\d{2}_\d{2})_(?P<name>[a-zA-Z0-9_]+)_(?P<public>[pP]ublic_){0,1}[pP]art(?P<part>[0-9]+)\.csv')
        d = c.match(f)
        
        if d == None:
            return {'resub':'','cc':'','date':'','name':'','public':'','part':''}
        return d.groupdict()
        
    def get_report_fnames(self, table_name):
        #c = re.compile('(?P<resub>Resubmission_){0,1}(?P<cc>[a-zA-Z0-9]+)_(?P<date>\d{8}|\d{4}_\d{2}_\d{2})_(?P<name>[a-zA-Z0-9_]+)_(?P<public>[pP]ublic_){0,1}[pP]art(?P<part>[0-9]+)\.csv')
        fname_root = self.tname_to_fname[table_name]
        
        fnames = []
        for f in os.listdir(self.indir):
            #d = c.match(f).groupdict()
            d = self.get_report_file_attrs(f)
            if d['name'] == fname_root:
                fnames.append(f)
        return fnames
        
    def read_report(self, table_name, chunksize=None, nrows=None, simplify_names=False, fix_names=False, groupby=None, agg_args=None, agg_piecewise=False):
        '''
        Read a TNC annual report
        '''
        files = self.get_report_fnames(table_name)
        
        dfs = []
        if chunksize==None:
            for f in files:
                df = pd.read_csv(os.path.join(self.indir,f))
                if simplify_names:
                    df.rename(columns={c:c.lower().replace(' ','').replace('_','').replace('-','') for c in df.columns}, inplace=True)
                if (isinstance(groupby, list) or isinstance(groupby, str)) and isinstance(agg_args, dict):
                    df = df.groupby(groupby, as_index=False).agg(agg_args)
                dfs.append(df)
                
            df = pd.concat(dfs)
            
            if fix_names and isinstance(self.data_dict, dict):
                df = fix_field_names(df, self.data_dict[table_name])
                
            if (isinstance(groupby, list) or isinstance(groupby, str)) and isinstance(agg_args, dict):
                df = df.groupby(groupby).agg(agg_args)
            yield df
        else:
            for f in files:
                for chunk in pd.read_csv(os.path.join(self.indir, f), chunksize=chunksize, nrows=nrows, encoding='cp1252'):
                    if fix_names and isinstance(self.data_dict, dict):
                        chunk = fix_field_names(chunk, self.data_dict[table_name])
                    yield chunk
                
                #try:
                #    yield next(pd.read_csv(os.path.join(self.indir, f), chunksize=chunksize))
                #except:
                #    
        
    #def read_all(self)
        