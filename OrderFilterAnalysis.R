#here is to check the range for order
getwd() #see the default path
setwd("/Users/user/Desktop/BigData9898") #set path
mfrow=c(2,2)
data<-read.csv("data-small.csv",header=F) #i have changed the file to csv format
data<-cbind(data,1:length(data[,1])) #add one more col to show its original order
data<-data[order(data[,1]),] #order based on date time
dist<-abs(data[,4]-1:length(data[,1])) #check the absolute difference between new and old 
#order
print(mean(dist))
print(median(dist))
print(sd(dist))
plot(1:length(data[,1]),dist,type="b",ylim=c(0,1300),main="order distance") #plot distance
#it seems that mostly, the distance between the wrong order and 
#the correct order ranges from 0 to 1200 based on the plot
#so it is Good Enough to set the block of 2400

#start to determine scrub algorithm
neg=data[data[,2]<=0,] #extract non-positive price data
n=length(data[,1])/100
plot(1:n*100,data[1:n*100,2],type='l',ylim=c(0,3000),main="price plot") #plot price data 
#to determine the 
#normal range
limited=data[data[,2]>0,] #only keep positive price data
limited=limited[limited[,2]<2500,] #only keep data with value under 2500
n=length(limited[,2])
limited=limited[limited[,3]>0,] #only keep positive traded units data
n=length(limited[,1])
timestep=limited[,1]
t=as.character.Date(timestep)
select<-TRUE
select2<- t[2:n]>t[1:n-1] #after ordering, remove disorder data
select<-c(select,select2)
finalp=limited[select,2] #extract order-filtered data
n=length(finalp)
hist(log(finalp)) #log price is approximately normal
x<-acf(log(finalp))
x$acf[1]<-NA
plot(x) 
#the autocorrelations are quite small, expecially for lag 1, I doubt that it is kind of too small.
#if the signal prices are real market data and the lags are usually within 1 second, the
#autocorrelations should be kind of greater
data[,4]=1:length(data[,4])
s<-data[data[,2]<=0 | data[,2]>2500,4]
select=(abs(s[2:length(s)]-s[1:length(s)-1])<=10)
print(sum(select))#0: 
#inidcate that extreme values may not follow one by one
# for asset price, usually one extreme value is followed by another similar extreme value
#so for such extreme values: price<=0 or price>2500, they are likely to be noise
pieces=10000
piecemean<-c(mean(log(finalp)))
piecesd<-c(sd(log(finalp)))
start=1
end=pieces
while(end<=length(finalp)){
  piecemean<-c(piecemean,mean(log(finalp[start:end])))
  piecesd<-c(piecesd,sd(log(finalp[start:end])))
  start=end+1
  end=end+pieces
}
plot(1:length(piecemean),piecemean,ylim=c(7.13,7.18)) #roughly stationary
piecesd<-piecesd[2:length(piecesd)]
plot(1:length(piecesd),piecesd,ylim=c(0.15,0.2)) #roughly stationary
#assume mean and std of fixed size price sample is stationary
#filter in blocks
max_bar=0#set the bar to determine abnormal price
max_block=0 #set filter block size
max_cri=0 #criterion is autocorrelation with lag 1
#here the filter algo tries to maximize autocorrelation with lag 1 for price data
maxsignal<-1
signal<-c(0)
bar1<-0
block<-0
quota<-0
for(bar1 in seq(1.6,2.6,0.1)){
  for(block in seq(500,1000,100)){
    signal<-c(0)
    start=1
    end=block
    quota=1.8*block*(1-pnorm(bar1)) #use discounted expected number
    #reason:this is simplied algo that does not consider the seriousness of abnormality
    #and the inclusion of noise data may inflate the real std
    while(end<=length(finalp)){
      quotaTaken=F
      count=0
      potential_s<-c(0)
      m=mean(log(finalp[start:end])) #get local mean
      s=sd(log(finalp[start:end])) #get local std
      z=abs(log(finalp[start:end])-m)/s #get z-score 
     for(i in start:end){
       if(z[i+1-start]<bar1){
        potential_s<-c(potential_s,finalp[i]) #keep normal data
       }
       else if(!quotaTaken){
              count=count+1 #count number of abnormal data
              if(count>=quota){
                quotaTaken=T #mark abnormal data above quota
               }
             }               
    }
    if(!quotaTaken){
      signal<-c(signal,finalp[start:end]) #if there is reasonable
      #number of abnormal data in start:end, no filter
     }
    else{
      #if abnormal data is more than quota, dump abnormal data
      potential_s<-potential_s[2:length(potential_s)]
      signal<-c(signal,potential_s)
    }
    start=end+1
    end=end+block
   }
  signal<-signal[2:length(signal)]
  x<-acf(log(signal))
  #find maximized autocorrelation
  if(x$acf[2]>max_cri){
      max_cri=x$acf[2]
      max_block=block
      max_bar=bar1
      print(max_cri)
      print(max_bar)
      print(max_block)
      maxsignal=signal
    }
  }
}
#the max_bar is 1.7, max_block is 900
x<-acf(log(maxsignal))
x$acf[1]<-NA
plot(x,main="after filter")
hist(log(maxsignal),main="after filter")

