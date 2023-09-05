'''
author: dcooper
date: 11/29/2022

custom utils for configparser
'''


def string_to_list(string, sep=',', strip=True, case=None):
    if strip == False and case == None:
        return string.split(sep)
    tmp = []
    for t in string.split(sep):
        if strip:
            t = t.strip()
        if isinstance(case, str):
            if case.lower() == 'lower':
                t = t.lower()
            if case.lower() == 'upper':
                t = t.upper()
        tmp.append(t)
    return tmp

# def read_config(path, list_vars=None, sep=',', bool_vars=None, float_vars=None, int_vars=None):
    # config = {}
        
    # cp = configparser.ConfigParser()
    # cp.read_file(f)
    
    # for sect, opt in list_vars.items():
        # v = cp.get(sect, opt)
        # config[opt] = string_to_list(v, sep)
        
    # for sect, opt in bool_vars.items():
        # config[opt] = cp.getboolean(sect, opt)
        
    # for sect, opt in float_vars.items():
        # config[opt] = cp.getfloat(sect, opt)
        
    # for sect, opt in int_vars.items():
        # config[opt] = cp.getint(sect, opt)
        
    # return config