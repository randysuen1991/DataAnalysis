from sklearn.decomposition import FactorAnalysis



class FactorModel():
    def __init__(self,n_components,**kwargs):
        self.fa = FactorAnalysis(n_components=n_components)
        
