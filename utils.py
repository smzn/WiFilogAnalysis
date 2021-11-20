import pandas as pd

def getCSV(file):
    return pd.read_csv(file, engine='python', encoding='utf-8')

def getCSVi(file, index_col):
    return pd.read_csv(file, engine='python', encoding='utf-8', index_col=index_col)


def saveCSV(df, fname):
    pdf = pd.DataFrame(df)
    pdf.to_csv(fname, index=False)

def saveCSVi(df, fname):
    pdf = pd.DataFrame(df)
    pdf.to_csv(fname, index=True)

def getDuplicate(df, column_name):
    return df.drop_duplicates(subset = column_name)
