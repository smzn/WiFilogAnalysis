import os
import pandas as pd
import numpy as np

import utils

class AfterTransition:

    def __init__(self, filepath, ap_file):
        self.filepath = filepath
        self.df_ap = utils.getCSV(ap_file)
        group_number = self.df_ap.iloc[-1]['buildID']
        self.group_transition = np.zeros((group_number, group_number))


    def csvAggregation(self, file_string, file_name):
        file_list = []
        for file in os.listdir(self.filepath):
            is_file = file_string in file
            not_csv_file = file_name != file
            if is_file and not_csv_file:
                file_list.append(file)
        print(file_list)

        if(file_string == 'transition'):
            for i in range(len(file_list)):
                if i == 0:
                    self.transition_all = utils.getCSVi(self.filepath+'/'+file_list[i], 0)
                else:
                    self.transition_all += utils.getCSVi(self.filepath+'/'+file_list[i], 0)
            utils.saveCSVi(self.transition_all, './after/'+file_name)

            transntion_rate = self.transition_all
            tsum = self.transition_all.sum(axis=1)
            for c in transntion_rate.columns:
                transntion_rate[c] = transntion_rate[c]/tsum
                transntion_rate = transntion_rate.fillna(0)
            utils.saveCSVi(transntion_rate, './after/transition_rate.csv')


        elif(file_string == 'duration'):
            for i in range(len(file_list)):
                if i == 0:
                    utils.getCSV(self.filepath+'/'+file_list[i]).to_csv('./after/'+file_name, columns=['client', 'from', 'to', 'duration'], index=False)
                else:
                    utils.getCSV(self.filepath+'/'+file_list[i]).to_csv('./after/'+file_name, columns=['client', 'from', 'to', 'duration'], index=False, mode='a')

    def getGroup(self, file_name):
        
        '''
        transition_all_np = self.transition_all.to_numpy()
        for i in range(len(transition_all_np)):
            print('i = {0}, from_group = {1}'.format(i, self.df_ap.iloc[i]['buildID']))
            for j in range(len(transition_all_np)):
                self.group_transition[self.df_ap.iloc[i]['buildID']-1][self.df_ap.iloc[j]['buildID']-1] += transition_all_np[i][j]
        utils.saveCSVi(np.nan_to_num(self.group_transition), './after/group_transition.csv')
        '''
        '''
        group_duration = utils.getCSV('./after/'+file_name)
        for i, row in enumerate(self.df_ap.itertuples()):
            group_duration.loc[group_duration['from'] == i, 'from'] = row.buildID
            group_duration.loc[group_duration['to'] == i, 'to'] = row.buildID
            print(str(i+1)+'/'+str(len(self.df_ap)))
        utils.saveCSV(group_duration, './after/group_duration.csv')
        '''

        group_duration = utils.getCSV('./after/'+file_name)
        self.df_ap['ind'] = [str(n) for n in range(len(self.df_ap))]
        group_duration = pd.merge(group_duration, self.df_ap[['ind', 'buildID']], left_on='from', right_on='ind', how='left').drop(['from', 'ind'], axis=1).rename(columns={'buildID': 'from'})
        group_duration = pd.merge(group_duration, self.df_ap[['ind', 'buildID']], left_on='to', right_on='ind', how='left').drop(['to', 'ind'], axis=1).rename(columns={'buildID': 'to'})
        group_duration = group_duration.reindex(columns=['client','from','to','duration'])
        utils.saveCSV(group_duration, './after/group_duration.csv')


if __name__ == '__main__':
    filepath = './main/'
    tfilename = 'transition_all_201407.csv'
    dfilename = 'duration_all_201407.csv'
    APfile = './after/APlocations_group.csv'
    aftertransition = AfterTransition(filepath, APfile)
    #aftertransition.csvAggregation('transition', tfilename)
    #aftertransition.csvAggregation('duration', dfilename)
    aftertransition.getGroup(dfilename)

    