import numpy as np
from numpy.linalg import solve
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import minimize, BFGS, LinearConstraint, NonlinearConstraint, Bounds

class Optimization_lib:
    def __init__(self, month, cluster, node, gamma, path, mu):
        self.month = month
        self.cluster = cluster
        self.node = node
        self.gamma = gamma #最適化でのmuの動く率
        self.path = path
        self.duration = np.zeros((self.cluster, self.node))
        self.mu = mu
        for i in range(cluster):
            self.duration[i] = np.array(pd.read_csv(self.path + self.month + '/ctmc/group_mean_duration_'+self.month+'_kmeans_'+str(i)+'.csv',index_col=0)).T[0]
        mu_mean = self.duration / 3600 #平均滞在時間(Hour)
        #平均滞在時間が0になってしまう(3時間で切っているから)場合は1時間とする
        self.mu_mean = np.where(mu_mean == 0, 1, mu_mean)
        #self.mu = 1 / self.mu_mean #滞在率
        self.K = np.zeros(self.cluster) #各クラスタの総和
        for i in range(self.cluster):
            self.K[i] = self.mu_mean[i].sum()
        #推移確率行列の取り込み
        self.p = np.zeros((self.cluster, self.node, self.node))
        for i in range(self.cluster):
          self.p[i] = pd.read_csv(self.path + self.month + '/ctmc/group_transition_rate_noout_'+self.month+'_kmeans_'+str(i)+'.csv', index_col=0).values
        
        #初期分布の定常分布を求めておく(クラスタごと)
        q = np.zeros((cluster, node, node))
        pi = np.zeros((cluster, node))
        for i in range(self.cluster):
            q[i] = self.getTransitionRate(self.mu[i*self.node:(i+1)*self.node], self.p[i])
            pi[i] = self.getStationary_solve(q[i])
        #目的関数の値(初期値)
        val0 = self.getObjective(pi)
        print('初期目的関数 : {0}'.format(val0))
        
        #等式制約
        self.cons = ({'type':'eq','fun':self.getConstraint})
        #bounds
        #mu = np.array([1.685405,2.55331003,2.36392575,12.96323733,2.59387581,2.14887887,3.02985132,1.4180166,1.8818737,0.95834197,11.65162425,1.98128168,2.82833905,4.62087429,1.5865729,1.41503574,1.93538965,3.00840175,3.57346757,3.89930942,1.15042735,2.63292581,1.62233407,3.03592357,5.40727237,8.84624577,6.14325481,2.03987221,5.9925005,2.38083003,9.34930824,1.67820917,2.69912117,2.08825954,2.20846571,4.1533306,1.11291008,0.84883703,0.82654892,1.25848023,0.7024883,1.07459608,2.00085862,1.66749012,1.23068175,1.39247668,5.41633781,1.29316916,0.90834102,1.74046502,0.76442079,4.82822656,0.51678486,3.63958044,1.52692101,1.63325006,2.06600388,2.26511967,1.35479222,0.82866488,1.12609158,1.87337381,4.12909448,0.94761779,1.73413353,0.71803464,1.74435809,6.4567373,4.01672666,3.29720653,4.55379166,2.85717885,2.15739665,8.09905317,0.87544737,2.96743013,0.85930653,1.77186493,3.77371369,0.95533678,0.3861418,0.51326651,1,0.46783626,0.37232392,1.99445983])
        self.lb = self.mu * (1 - self.gamma)
        self.ub = self.mu * (1 + self.gamma)
        #print(self.lb)
        #print(self.ub)
        
    #推移率行列を作成する関数
    def getTransitionRate(self, mu, p):
        q = p.copy()
        #推移率行列を求める
        #(1)サービス率と推移確率との積をとる
        for i in range(len(q)):
            q[i] *= mu[i]
        #(2)対角要素に行和のマイナス値を入れる
        for i in range(len(q)):
            q[i][i] = np.sum(q[i]) * (-1)
        return q
        
    #推移率確率行列から定常分布を求める関数
    def getStationary_solve(self, q):#numpy.solveを使う場合
        #定常分布を求める
        q1 = q.copy()
        #(3)最終列に1を代入
        right = [0 for i in range(len(q))]
        right[-1] = 1 #最後の要素のみ1にする
        q1[:,-1] = 1 #最終列を1にする
        #(4)連立方程式を解く πP=0 => P^tπ=0
        e = np.eye(len(q)) #次元を1つ小さくする
        pi = solve(q1.T, right)
        return pi
        
    #目的関数
    def getObjective(self, pi):
      return np.var(pi)
    
    #制約条件  
    def getConstraint(self, mu):
        #mu = np.array([1.685405,2.55331003,2.36392575,12.96323733,2.59387581,2.14887887,3.02985132,1.4180166,1.8818737,0.95834197,11.65162425,1.98128168,2.82833905,4.62087429,1.5865729,1.41503574,1.93538965,3.00840175,3.57346757,3.89930942,1.15042735,2.63292581,1.62233407,3.03592357,5.40727237,8.84624577,6.14325481,2.03987221,5.9925005,2.38083003,9.34930824,1.67820917,2.69912117,2.08825954,2.20846571,4.1533306,1.11291008,0.84883703,0.82654892,1.25848023,0.7024883,1.07459608,2.00085862,1.66749012,1.23068175,1.39247668,5.41633781,1.29316916,0.90834102,1.74046502,0.76442079,4.82822656,0.51678486,3.63958044,1.52692101,1.63325006,2.06600388,2.26511967,1.35479222,0.82866488,1.12609158,1.87337381,4.12909448,0.94761779,1.73413353,0.71803464,1.74435809,6.4567373,4.01672666,3.29720653,4.55379166,2.85717885,2.15739665,8.09905317,0.87544737,2.96743013,0.85930653,1.77186493,3.77371369,0.95533678,0.3861418,0.51326651,1,0.46783626,0.37232392,1.99445983])
        Ts = np.zeros(self.cluster)
        for i in range(self.cluster):
            #print(mu[i*self.node:(i+1)*self.node].shape)
            for j in mu[i*self.node:(i+1)*self.node]:
                Ts[i] += 1/j    
            #Ts[i] = [1/j for j in mu[i*self.node:(i+1)*self.node]]#平均時間に変換 ここを直す
        #print(self.K - np.sum(Ts))
        #return self.K - np.sum(Ts)
        print(Ts)
        #print(self.K - Ts)
        return self.K - Ts
      
    #目的関数 (サービス率を変更して、再度定常分布を求め、目的関数を求める)
    def getOptimize(self, mu):
        q = np.zeros((self.cluster, self.node, self.node))
        pi = np.zeros((self.cluster, self.node))
        for i in range(self.cluster):
            #(1)推移確率行列を求める
            q[i] = self.getTransitionRate(mu[node*i:node*(i+1)], self.p[i])
            #(2)定常分布を求める
            pi[i] = self.getStationary_solve(q[i])
        #(3)目的間数値を求める
        #print(self.getObjective(pi))
        return self.getObjective(pi)
      
    def executeOptimize(self):
        # 最適化
        mu = self.mu
        #mu = np.array([1.685405,2.55331003,2.36392575,12.96323733,2.59387581,2.14887887,3.02985132,1.4180166,1.8818737,0.95834197,11.65162425,1.98128168,2.82833905,4.62087429,1.5865729,1.41503574,1.93538965,3.00840175,3.57346757,3.89930942,1.15042735,2.63292581,1.62233407,3.03592357,5.40727237,8.84624577,6.14325481,2.03987221,5.9925005,2.38083003,9.34930824,1.67820917,2.69912117,2.08825954,2.20846571,4.1533306,1.11291008,0.84883703,0.82654892,1.25848023,0.7024883,1.07459608,2.00085862,1.66749012,1.23068175,1.39247668,5.41633781,1.29316916,0.90834102,1.74046502,0.76442079,4.82822656,0.51678486,3.63958044,1.52692101,1.63325006,2.06600388,2.26511967,1.35479222,0.82866488,1.12609158,1.87337381,4.12909448,0.94761779,1.73413353,0.71803464,1.74435809,6.4567373,4.01672666,3.29720653,4.55379166,2.85717885,2.15739665,8.09905317,0.87544737,2.96743013,0.85930653,1.77186493,3.77371369,0.95533678,0.3861418,0.51326651,1,0.46783626,0.37232392,1.99445983])
        res = minimize(self.getOptimize,                       # 目的関数, 
                        #self.mu[0:2].reshape(1,self.node*2),                     # 初期解,
                        mu,
                        method="trust-constr",  # 制約付き信頼領域法
                        jac="2-point",          # 勾配関数
                        hess=BFGS(),            # ヘシアンの推定方法
                        constraints=self.cons,      # 制約
                        bounds=Bounds(self.lb, self.ub),
                        options={"maxiter": 30, # 最大反復数
                                 "verbose":2})  # 最適化の過程を出力
        print('最適化結果')                         
        print(res["x"])
        #最終結果
        q = np.zeros((self.cluster, self.node, self.node))
        pi = np.zeros((self.cluster, self.node))
        for i in range(self.cluster):
            q[i] = self.getTransitionRate(res["x"][i*self.node:(i+1)*self.node], self.p[i])
            pi[i] = self.getStationary_solve(q[i])
        #q = getTransitionRate(transition[0], res['x'])
        #pi = getStationary_solve(q)
        val = self.getObjective(pi)
        print('定常分布pi = {0}'.format(pi))
        print('目的間数値(最終) : {0}'.format(val))
        
if __name__ == '__main__':
    path = '/content/drive/MyDrive/研究/WiFiLog/wifidata/cluster/'
    month = '201409'
    cluster = 5
    node = 43
    gamma = 0.25 #最適化でのmuの動く率
    mu = np.array([0.5933292,0.39164848,0.42302513,0.07714122,0.38552347,0.46535894,0.3300492,0.70521036,0.53138529,1.04346886,0.08582494,0.50472379,0.3535644,0.21640926,0.63028935,0.70669593,0.51669182,0.33240241,0.27984023,0.25645567,0.8692422,0.37980561,0.61639586,0.32938906,0.18493613,0.1130423,0.16278016,0.49022679,0.16687525,0.42002158,0.10695979,0.59587328,0.37049096,0.47886768,0.45280305,0.24077062,0.89854519,1.17808244,1.20984974,0.79460923,1.42351126,0.93058221,0.49978544,0.5997037,0.81255776,0.71814488,0.18462659,0.77329404,1.10090812,0.57455909,1.30818001,0.20711538,1.9350412,0.27475694,0.65491273,0.61227611,0.4840262,0.44147778,0.73812057,1.20676045,0.88802724,0.5337963,0.24218385,1.05527778,0.57665686,1.39269047,0.57327679,0.15487698,0.24895894,0.30328704,0.21959722,0.34999559,0.46352163,0.12347122,1.14227312,0.33699193,1.16372909,0.5643771,0.26499096,1.04675128,2.58972222,1.94830556,1,2.1375,2.68583333,0.50138889,0.46085217,0.56658689,0.52853075,0.07059843,0.53340854,0.82374086,0.48808808,0.93655125,0.32128326,1.36983368,0.08862976,0.48203977,0.39677644,0.21410864,0.28948281,0.76309225,0.62189127,0.59758383,0.33849139,0.13497157,1.40165242,0.34500128,1.14429388,0.3686188,0.11720124,0.12101403,0.27427954,0.25268053,0.19520594,0.35455828,0.08788379,0.79984759,0.22569514,0.89887049,0.431158,0.21954091,0.87141355,0.95340278,1.80047653,1.26648148,1.97633333,2.1147619,0.5298569,0.47595833,1.36148222,0.84578826,0.10660301,1.45681983,1.05141919,0.81416464,0.97713818,0.383,1.27221865,0.12597222,0.82541336,0.81630007,0.50163185,0.77738965,1.26273693,0.90320227,0.71846399,0.81119949,0.53297127,1.14996032,0.68562589,0.88254952,0.61165176,0.48969674,0.08043651,0.50347101,0.73474054,0.35314459,0.60235169,0.08545364,1.02253345,0.50432261,0.99670752,0.79183145,0.34817847,1.34144259,1.63083333,1.69638889,1,1.44162698,1.91944444,0.69458333,0.41694444,0.60644678,0.57700674,0.0875761,0.6729523,0.69670894,0.58816419,1.69905762,0.43023148,1.34354515,0.05727431,0.47831231,0.39447858,0.25225834,0.47181495,0.84498588,0.57691822,0.56482741,0.39643563,0.16644796,1.37144444,0.33535073,1.30854438,0.41642521,0.27634719,0.08594172,0.16190369,0.24422076,0.1554472,0.48127008,0.12852522,0.65933659,0.23806096,0.91674824,0.43499278,0.19979561,0.96351218,1,1.75472222,1.23638889,1.99430556,2.15263889,0.61708333])
    olib = Optimization_lib(month, cluster, node, gamma, path, mu)
    olib.executeOptimize()
    