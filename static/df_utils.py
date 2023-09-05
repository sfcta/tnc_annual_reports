'''
author: Drew Cooper
date: January 7, 2020
description: some stuff you can do with pandas DataFrames
'''
def as_percent(df, cols=None, axis=None):
    if not cols:
        cols = df.columns.tolist()
    d = df.loc[:,cols].copy()
    if axis==None:
        s = d.sum().sum()
        return d/s
    else:
        s = d.loc[:,cols].sum(axis)
        if axis==1:
            for c in cols:
                d[c] = d[c] / s
        elif axis==0:
            for c in cols:
                d[c] = d[c] / s[c]
    return d

def df_to_excel(writer, sheet_name, df, table_name, 
                      startrow, startcol, 
                      row_buffer=2, col_buffer=2,
                      home_row=0, home_col=0,
                      offset_rows=False, offset_cols=False,
                      reset_rows=False, reset_cols=False,
                      index=True
                      ):
    
    workbook = writer.book
    title_fmt = workbook.add_format({'bold':True,'italic':True})
    firstrow_fmt = workbook.add_format({'bottom':2,'top':1,'bold':True})
    oddrows_fmt = workbook.add_format({'bottom':0,'top':0})
    evenrows_fmt = workbook.add_format({'bottom':0,'top':0}) 
    lastrow_fmt = workbook.add_format({'bottom':2})
    firstcol_fmt = workbook.add_format({'right':1})
    
    # write the df first, at startrow +1, in case the sheet doesn't exist yet
    df.to_excel(writer, sheet_name=sheet_name, startrow=startrow+1, startcol=startcol, index=index)

    # then go back and write the title
    worksheet = writer.sheets[sheet_name]
    #worksheet.conditional_format(startrow+1,startcol,startrow+1,startcol+len(df.columns)-1,
    #                             {'type': 'cell', 'criteria':'>=', 'value':0, 'format': firstrow_fmt})
    #worksheet.conditional_format(startrow+2,startcol,startrow+len(df),startcol+len(df.columns)-1,
    #                             {'type': 'cell', 'criteria':'>=', 'value':0, 'format': oddrows_fmt})
    #worksheet.conditional_format(startrow+len(df)+1,startcol,startrow+1,startcol+len(df.columns)-1,
    #                             {'type': 'cell', 'criteria':'>=', 'value':0, 'format': lastrow_fmt})
    worksheet.write(startrow, startcol, table_name, title_fmt)
    
    if reset_rows:
        startrow = home_row
    elif offset_rows:
        startrow = startrow + len(df) + 2 + row_buffer
    if reset_cols:
        startcol = home_col
    elif offset_cols:
        startcol = startcol + len(df.columns) + 2 + col_buffer
    

    return startrow, startcol
    
#def set_range_styles