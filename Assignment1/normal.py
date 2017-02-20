'''accept time series in bytearray version'''
'''here I assume all prices data in time series are positive and valid and in correct format'''
import math
from scipy import stats
'''if the return value is not very small, it can be assumed to be normal'''
def getJarque_Bera_test_pval(sample_size,skew,kurt):
    #calculate test stat for JB test
    test_stat=(sample_size/6.0)*(skew**2+0.25*(kurt-3.0)**2)
    #return p value
    return 1.0-stats.chi2.cdf(test_stat,2)

def getStandardDeviation(moment1,moment2):
    #get standard deviation
    return math.sqrt(moment2-moment1**2)

def getSkew(moment1,moment2,moment3):
    sd=getStandardDeviation(moment1,moment2)
    third_central_m=moment3-3*moment1*moment2+2*moment1**3
    #get skew
    return third_central_m/(sd**3)

def getKurtosis(moment1,moment2,moment3,moment4):
    sd=getStandardDeviation(moment1,moment2)
    forth_central_m=moment4+6*(moment1**2)*moment2-4*moment1*moment3-3*moment1**4
    #return kurtosis
    return forth_central_m/(sd**4)


class Normal_stat(object):
    def __init__(self,time_series_bytearr, isPrice): #if the passed-in series are return, set False
        lines=time_series_bytearr.split(b"\n")
        #initialize moments
        self.moment1=0
        self.moment2=0
        self.moment3=0
        self.moment4=0
        prev_p=None
        count=0
        for line in lines:
            items=line.split(b",")
            try:
                #get price or return
                currentp=float(items[1])
                if prev_p is None:
                  prev_p=currentp
                else:
                  r=currentp
                  if isPrice:
                    r=math.log(currentp/prev_p)
                  #increment moments to sum them up
                  self.moment1+=r
                  self.moment2+=r**2
                  self.moment3+=r**3
                  self.moment4+=r**4
                  prev_p=currentp
                  count+=1
            except:
                continue
        self.n=count
        #divide by sample size
        self.moment1/=count
        self.moment2/=count
        self.moment3/=count
        self.moment4/=count
    #functions to get statistics of the return series
    def moments_and_size(self):
        return (self.n,self.moment1,self.moment2,self.moment3,self.moment4)
    def getMean(self):
        return self.moment1
    def getStandardDeviation(self):
        return math.sqrt(self.moment2-self.moment1**2)
    def getSkew(self):
        sd=self.getStandardDeviation()
        third_central_m=self.moment3-3*self.moment1*self.moment2+2*self.moment1**3
        return third_central_m/(sd**3)
    def getKurtosis(self):
        sd=self.getStandardDeviation()
        forth_central_m=self.moment4+6*(self.moment1**2)*self.moment2-4*self.moment1*self.moment3-3*self.moment1**4
        return forth_central_m/(sd**4)
