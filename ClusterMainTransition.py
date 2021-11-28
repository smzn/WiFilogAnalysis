import pandas as pd
import datetime as dt
import time
from mpi4py import MPI

import sys
sys.path.append('/content/drive/MyDrive/研究/WiFiLog/')
import utils

class ClusterMainTransition:

    def __init__(self, ap_file, data_file, user_file, cluster, calmonth, rank, size, path, comm):
        self.df_ap = utils.getCSV(ap_file)
        print(self.df_ap.head())
        self.cluster = cluster
        print('Cluster {}'.format(self.cluster))
        self.user = utils.getCSV(user_file)
        self.user_list = self.user[self.user['kmeans']==self.cluster]['client'].tolist()
        self.df_data = utils.getCSV(data_file)
        self.df_data = self.df_data[self.df_data['client'].isin(self.user_list)]
        print(self.df_data.head())
        self.calmonth = calmonth
        self.rank = rank
        self.size = size
        self.path = path
        self.comm = comm
        self.df_unique = utils.getDuplicate(self.df_data, 'client')
        print('ユニーク利用者数 : ' +str(len(self.df_unique)))

        self.transition_from = []
        self.transition_to = []
        self.duration = []
        self.client = []
        self.transition = [[0 for i in range(len(self.df_ap)+1)] for i in range(len(self.df_ap)+1)]
        

    def getTransition(self):
        #3.
        l_div = [(len(self.df_unique['client'])+i) // self.size for i in range(self.size)]
        print(l_div)
        start = 0
        end = 0
        for i in range(self.rank):
            start += l_div[i]
        for i in range(self.rank + 1):
            end += l_div[i]
        div_unique = self.df_unique[start:end]
        print('rank = {0}, size = {1}, start = {2}, end = {3}'.format(self.rank, self.size, start, end))

        #for i, val in enumerate(self.df_unique['client']):
        for i, val in enumerate(div_unique['client']):
            #print(str(i) + '/' + str(len(self.df_unique)))
            print(str(i) + '/' + str(len(div_unique)))
            from_ap = len(self.df_ap)
            to_ap = -1
            for j in self.df_data.itertuples():
                if(j.client == val):
                    if(to_ap == -1):
                        to_ap = j.AP
                        from_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                        to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                        self.transition_from.append(from_ap)
                        self.transition_to.append(to_ap)
                        self.duration.append(-1)
                        self.client.append(val)
                    elif(to_ap >= 0):
                        if(to_ap == j.AP):
                            to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                        else:
                            self.transition_from.append(to_ap)
                            self.transition_to.append(j.AP)
                            to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                            delta = to_time - from_time
                            self.duration.append(delta.total_seconds())
                            self.client.append(val)

                            from_ap = to_ap
                            to_ap = j.AP
                            from_time = to_time

            self.transition_from.append(to_ap)
            self.transition_to.append(len(self.df_ap))
            delta = to_time - from_time
            self.duration.append(delta.total_seconds())
            self.client.append(val)

            #if(i > 30):
            #    break
                
        #データの集約
        if self.rank == 0:
            for i in range(1, self.size):
                client = self.comm.recv(source=i, tag=00)
                transition_from = self.comm.recv(source=i, tag=10)
                transition_to = self.comm.recv(source=i, tag=11)
                duration = self.comm.recv(source=i, tag=20)
                print('receive(client) : {0}, {1}'.format(i, client))
                print('receive(transition_from) : {0}, {1}'.format(i, transition_from))
                print('receive(transition_to) : {0}, {1}'.format(i, transition_to))
                print('receive(duration) : {0}, {1}'.format(i, duration))

                #リストの結合
                self.client.extend(client)
                self.transition_from.extend(transition_from)
                self.transition_to.extend(transition_to)
                self.duration.extend(duration)
        else:
            self.comm.send(self.client, dest=0, tag=00)
            self.comm.send(self.transition_from, dest=0, tag=10)
            self.comm.send(self.transition_to, dest=0, tag=11)
            self.comm.send(self.duration, dest=0, tag=20)
        self.comm.barrier() #プロセス同期 (必要無いかも)
        #並列化ここまで

        #4. 
        df = pd.DataFrame({'client': self.client, 'from': self.transition_from, 'to': self.transition_to, 'duration': self.duration})
        utils.saveCSV(df, self.path +'/cluster/'+str(self.calmonth)+'/duration_'+str(self.calmonth)+'_kmeans_'+str(self.cluster)+'.csv')

        #5.
        for i, j in zip(self.transition_from, self.transition_to):
            self.transition[i][j] += 1
        utils.saveCSVi(self.transition, self.path +'/cluster/'+str(self.calmonth)+'/transition_'+str(self.calmonth)+'_kmeans_'+str(self.cluster)+'.csv')

        
if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    path = '/content/drive/MyDrive/研究/WiFiLog/wifidata'
    calmonth = '201407'
    cluster = 2
    start = time.time()
    maintransition = ClusterMainTransition(path +'/traceset/APlocations.csv', path +'/pre/'+calmonth+'.csv', path + '/cluster/'+calmonth+'/users_kmeans_'+calmonth+'.csv', cluster, calmonth, rank, size, path, comm)
    maintransition.getTransition()
    if rank == 0:
        print('Done!')
        elapsed_time = time.time() - start
        print ("calclation_time:{0}".format(elapsed_time) + "[sec]")
