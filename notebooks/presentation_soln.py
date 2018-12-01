import neuralnetworks as nn
import mlutils as ml
import pandas as pd
import numpy as np 
import random
import seaborn as sns
import matplotlib.pylab as plt
from matplotlib.pyplot import cm
import dill

class Models:
    
    def __init__(self, num_districts, networks):
        """Initialize number of districts and repsective networks"""
        self.num_districts = num_districts
        self.networks = networks
        self.trained_nn = []
        
    def sampler(self, data, features, targets):
        """Sample the data to set features, X, and targets, T."""
        X = data.iloc[:, np.r_[features]]
        T = data.iloc[:, np.r_[targets]]
        return np.array(X), np.array(T)

    def train(self, iterations=1000, normalize=True, partition=False):
        """Train each network and save the trained nnet object to trained_nn,
        Use all the values for training, i.e., partition=False"""
        for d in range(self.num_districts):
            data = pd.read_csv('../output/WeeklyOutput/wc'+str(d + 1)+'.csv', sep=',', low_memory=False, 
            names = ['date', 'dry', 'wet', 'wind', 'humidity', 'district', 'homicide', 'robbery',
                     'battery', 'assault', 'burglary', 'theft', 'motor', 'weapons']).iloc[70:]
            if normalize:
                cols_to_norm = ['dry', 'wet', 'wind', 'humidity']
                data[cols_to_norm] = data[cols_to_norm].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

            X, T = sampler(data, range(1,5), range(6,14))
        
            if partition: 
                train_f = 0.80
                Xtrain, Ttrain, _, _ = ml.partition(X, T, (train_f, 1 - train_f))

                nnet = nn.NeuralNetwork(Xtrain.shape[1], self.networks[d], Ttrain.shape[1])
                nnet.train(Xtrain, Ttrain, iterations)
            else:  
                nnet = nn.NeuralNetwork(X.shape[1], self.networks[d], T.shape[1]) 
                nnet.train(X, T, iterations)
            
            self.trained_nn.append(nnet)
    
    def use(self, data, return_all=True, district=None):
        """Prints a table of all or specific district results based off provided data.
        The data shall be the weather for the day in the form:
        [dry-bulb-temp, wet-bulb-temp, wind-speed, relative-humidity]"""
        results = []
        _index = range(1, 10)
        
        if return_all:
            for i, network in enumerate(self.trained_nn):
                Y = np.round(network.use(data)[0])
                results.append(np.absolute(Y))
        elif district is not None:
            Y = np.round(self.trained_nn[district - 1].use(data)[0])
            results.append(np.absolute(Y))
            _index = district
        else:
            print('ERROR, return_all = True, or set to false and specify district number.')
        df = pd.DataFrame(results, columns=['Homicide', 'Robbery', 'Battery', 'Assault',
                                            'Burglary', 'Theft', 'Motor Theft', 'Weapons Assault'],
                          index=_index).astype(int)
        
        district_names = ['Far North Side', 'North Side', 'N-W Side', 'Central', 'West Side',
                          'S-W Side', 'South Side', 'Far S-W Side', 'Far West Side']
        for i in _index:
            print('| {0}:{1:>15} '.format(i, district_names[i-1]), end='')
            if i == 5:
                print()  
        print('|')
        
        return df.head(10)

    
def save_dill(iterations=1000, normalize=True, partition=False):
    """Train Models class with defined networks.
    Then save class to a .pickle file"""
    networks = [
                [5], # district 1
                [5], # district 2
                [5], # district 3
                [5], # district 4
                [5], # district 5
                [5], # district 6
                [5], # district 7
                [5], # district 8
                [5], # district 9
               ]

    M = Models(len(networks), networks)
    M.train(iterations, normalize, partition)

    outfile = open('trained_models.pickle', 'wb')
    dill.dump(M, outfile)
    outfile.close()
    
def sampler(data, a, b):
    """Sample the data to set features, X, and targets, T."""
    X = data.iloc[:, np.r_[a]]
    T = data.iloc[:, np.r_[b]]
        
    return np.array(X), np.array(T)

def get_values(X, T, network, train_f, itr, partition = False):
    """Get test results and error trace"""
    if partition: 
        Xtrain, Ttrain, Xtest, T = ml.partition(X, T, (train_f, 1 - train_f))
        
        nnet = nn.NeuralNetwork(Xtrain.shape[1], network, Ttrain.shape[1])
        nnet.train(Xtrain, Ttrain, itr)
        Y = nnet.use(Xtest)
        
    else:  
        nnet = nn.NeuralNetwork(X.shape[1], network, T.shape[1]) 
        nnet.train(X, T, itr)
        Y = nnet.use(X)
        
    return Y, T, nnet.getErrorTrace()

def network_test(district='4', ns=10):
    """Display a run with 20 networks - graphing the error output"""
    data = read_data(district).iloc[70:]
    
    cols_to_norm = ['dry', 'wet', 'wind', 'humidity']
    data[cols_to_norm] = data[cols_to_norm].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

    X, T = sampler(data, range(1, 5), range(6,14))

    sns.set_style("whitegrid")

    numberItr = 1200
    train_f = 0.8
    er = []
    networks = []
    for i in range(ns):
        a = random.sample(range(1, 100), np.random.randint(1, 6))
        Y, _T, error = get_values(X, T, a, train_f, numberItr)
        er.append(error)
        networks.append(a)
        numberItr = int(1.05 * numberItr)

    color=iter(cm.rainbow(np.linspace(0,1,ns)))
    plt.figure(figsize=(18,8))
    for i, pl in enumerate(er):
        plt.plot(pl, c=next(color), label = 'Network '+str(networks[i]))

    plt.xlabel('Iteration')
    plt.ylabel('Error')
    plt.legend()
    plt.show()
    
def bar_test(year=5, district='4', daily=False):
    """Display the crime data for a given year and district"""
    plt.style.use('default')
    def bar_crime(data, year, daily):
        # col = ['homicide', 'robbery','battery', 'assault', 'burglary', 'theft', 'motor', 'weapons']
        col = ['homicide', 'battery', 'assault', 'theft']
        plt.figure(figsize=(18,25))
        
        bags = 52
        linespace_s = 50
        if daily is True:
            bags = 365
            linespace_s = 500
            
        for i in range(len(col)):
            plt.subplot(4,2,i+1)  
            y = data[col[i]][bags*(year - 1):bags*year]
            x = np.arange(len(y))
            z = np.polyfit(x, y, 3)

            p = np.poly1d(z)
            p30 = np.poly1d(np.polyfit(x, y, 15))
            xp = np.linspace(0, len(y) - 1, linespace_s)

            plt.bar(x, y, color='tan')
            _ = plt.plot(xp, p(xp), 'k--', xp, p30(xp), 'b-', lw=2.5)
            plt.title(col[i])
            plt.xlabel('days (samples)'), plt.ylabel('num crimes') 

        plt.show() 
        
    if daily is True:
        data = pd.read_csv('../output/DailyOutput/wc'+district+'.csv', sep=',', low_memory=False, 
               names = ['date', 'dry', 'wet', 'wind', 'humidity', 'district', 'homicide', 'robbery',
                        'battery', 'assault', 'burglary', 'theft', 'motor', 'weapons'])
        # SORT BY DATA
        #data['date'] = pd.to_datetime(data.date)
        #data.sort_values(by='date', inplace=True)
        bar_crime(data, year, daily)
    else:
        data = read_data(district)
        bar_crime(data, year, daily)

    
def read_data(district='5', usecols=range(14)):
    return pd.read_csv('../output/WeeklyOutput/wc'+district+'.csv', sep=',', low_memory=False, 
                names = ['date', 'dry', 'wet', 'wind', 'humidity', 'district', 'homicide', 'robbery',
                         'battery', 'assault', 'burglary', 'theft', 'motor', 'weapons'], usecols=usecols)
def data_tail():
    return read_data().tail(1)

def crime_head():
    return pd.read_csv('../archive/sample/crime_records.csv', sep=',').head(1)
                
def weather_head():
    return pd.read_csv('../archive/sample/weather.csv', sep=',').iloc[:, : 19].head(1)

def feature_violin(num, columns, figheight, subplot_1, subplot_2):
    """Combination of boxplot and kernel density estimate for Wx data.
    May need to adjust figsize and subplot to fit more graphs.
    """
    #sns.set_style('whitegrid')
    plt.figure(figsize=(18,figheight))
    for i in range(num):
        plt.subplot(subplot_1,subplot_2,i+1)
        weather = read_data(usecols=columns)
        plt.title('Boxplot and Kernel Density Estimate')
        sns.violinplot(data=weather)

    plt.show()
