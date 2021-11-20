import pandas as pd
import datetime as dt
import time

import utils

class PreTransition:
    def __init__(self, ap_file, data_file, calmonth):
        self.df_ap = utils.getCSV(ap_file)
        self.df_data = utils.getCSV(data_file)
        self.calmonth = calmonth
        
        
    def getPreTransition(self):
        #2
        print(self.df_data.isnull().sum())
        self.df_data = self.df_data[self.df_data['AP'].isnull() == False]
        print(self.df_data.isnull().sum())

        #3
        grouped = self.df_data.groupby('client').size()
        print(grouped)
        l_columns = list(grouped[grouped > 1].index)
        self.df_data_over2 = self.df_data[self.df_data['client'].isin(l_columns)].copy()
        grouped2 = self.df_data_over2.groupby('client').size().sort_values(ascending=True)
        print(grouped2)

        #4
        df_ap_index = pd.DataFrame({'AP': self.df_ap.AP, 'AP_index': range(len(self.df_ap.AP))})
        self.df_data_over2 = self.df_data_over2.merge(df_ap_index, on = 'AP', how = 'left')
        self.df_data_over2.drop(columns='AP', inplace=True)
        self.df_data_over2.rename(columns={'AP_index': 'AP'}, inplace=True)
        print(self.df_data_over2)

        #5
        utils.saveCSV(self.df_data_over2, './pre/'+str(self.calmonth)+'.csv')

    def getClientsize(self):
        grouped = self.df_data.groupby('client').size().sort_values(ascending=False)
        utils.saveCSVi(grouped, './pre/clientsize_'+str(self.calmonth)+'.csv')


if __name__ == '__main__':
    pretransition = PreTransition('./traceset/APlocations.csv', './traceset/2014_07.csv', '201407')
    start = time.time()
    pretransition.getPreTransition()
    elapsed_time = time.time() - start
    print("calclation_time:{0}".format(elapsed_time)+"[sec]")
    pretransition.getClientsize()

 