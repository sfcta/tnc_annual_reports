import pandas as pd

def find_name_match(name, candidates):
    for c in candidates:
        simp_name = str(name).lower().strip().replace('_','').replace(' ','').replace(r'ï»¿','')
        simp_c = str(c).lower().strip().replace('_','').replace(' ','')
        
        simp_c = simp_c.replace('harrass','harass')
        
        if simp_c == 'totalmilesmt':
            simp_c = 'totalmilesmth'
        if simp_c == 'meanmilesmt':
            simp_c = 'meanmilesmth'
        if simp_c == 'medianmilesmt':
            simp_c = 'medianmlesmth'
        if simp_c == 'carrierid':
            simp_c = 'tncid'
        
        if ((simp_name == simp_c) or 
            (simp_name.replace('hours','miles') == simp_c) or 
            (simp_name.replace('miles','hours') == simp_c) or
            (simp_name.replace('assaut','assault') == simp_c)):
            if str(name) == str(c):
                return (c, 'exact')
            else:
                return (c, 'approx')
                
    return (None, None)
    
def fix_field_names(report, data_dict):
    rename = {}
    for c in report.columns.tolist():
        m, t = find_name_match(c, data_dict.index.tolist())
        if m != None:
            rename[c] = m
    out = report.rename(columns=rename)
    return out
    
def dtype_mismatch(value, cpuctype):
    if cpuctype in ['Alpha','AlphaNumeric']:
        if not isinstance(value, str):
            return True
    else:
        try:
            value = pd.to_datetime(value)
        except:
            pass
        if isinstance(value, str):
            return True
    return False

def len_mismatch(value, length, precision=None):
    if len(str(value)) != length:
        return True
        
def read_data_dictionary(path, sheet_names=None):
    '''
    Read the data dictionary (dd) into a dict of name -> data dictionary
    '''
    # 
    # skip the first row.  Cell A1 has a report description, all others are blank
    dd = pd.read_excel(path, sheet_name=sheet_names, skiprows=1, index_col=0)
    dd.pop('index')
    return dd
    
def get_report_descriptions(path, sheet_names=None):
    # read the first row only to get the report description
    tmp = pd.read_excel(path, sheet_name=sheet_names, nrows=0)
    desc = {}
    for k, v in tmp.items():
        desc[k] = v.columns[0]
    return desc