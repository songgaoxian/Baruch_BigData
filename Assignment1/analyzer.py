from mpi4py import MPI
import numpy
import re
import math

class Analyzer(object):
    def __init__(self,b_arrays,lot_size,noise_fh,signal_fh):
        self.lot_size=lot_size #number of lines to write in batch
        self.noise_time=0
        self.sig_time=0
        lines=b_arrays.split(b"\n") #split bytearrays
        # initial filter---remove invalid data
        i=0
        self.n = len(lines)
        self.order_block=0 #should be the number data per second
        get_1_s=False
        self.data = numpy.empty([self.n, 3], dtype="S25") #initialize data
        self.lot_size=lot_size
        self.noise_fh=noise_fh
        self.signal_fh=signal_fh
        count_noise=0
        initial_time=None
        noise=b""
        self.io_time=0
        '''negative values are considered as bad format'''
        for i in range(self.n):
            #check format
            if re.match(b'(\d{8}):\d{2}:\d{2}:\d{2}.\d{6},\d+.\d+,\d+',lines[i]) is not None:
                lines[i]=lines[i].strip()
                self.data[i]=lines[i].decode().split(",")
                if get_1_s==False: #has not got 1 second data
                    self.order_block+=1
                    if initial_time is None:
                        initial_time=self.data[i,0].split(b".")[0] #set initial time
                    else:
                        get_1_s=(initial_time!=self.data[i,0].split(b".")[0]) #update get_1_s
            else:
                self.data[i]=["","",""] #it means data is valid
                count_noise+=1#increment noise count
                noise=noise+lines[i].strip()+b" flag: bad format "+b"\n" #concatenate with noise
                tic=MPI.Wtime()
                if count_noise>=self.lot_size:
                    noise_fh.Write_ordered(noise) #if batch is filled, write to noise file
                    count_noise=0
                    noise=b""
                toc=MPI.Wtime()
                self.io_time+=toc-tic
                self.noise_time+=toc-tic
        self.order_block=max(5000,self.order_block)#5000 is set by analysis in R
        tic=MPI.Wtime()
        if count_noise>0:
            noise_fh.Write_ordered(noise) #writ remaining noise to file
        toc=MPI.Wtime()
        self.io_time+=toc-tic
        self.noise_time+=toc-tic


    def block_ordering(self):
        start=0
        end=self.order_block
        while end<self.n:
            self.data[start:end].sort(axis=0) #sort data in batch size of self.order_block
            start=end
            end=end+self.order_block
        #remove duplicates
        #duplicates are not regarded as noise
        for i in range(1,self.n):
            if self.data[i,0]==b"": continue
            compare=(self.data[i]==self.data[i-1])
            if compare[0] and compare[1] and compare[2]:
                self.data[i-1]=["","",""]

    #here is to filter extreme values
    def filter_extremes(self,upper,lower):
        #remove extreme values
        count_noise=0
        noise=b""
        for i in range(self.n):
            if self.data[i,0]==b"":continue
            #if data is out of the boundary, treat it as noise
            if float(self.data[i,1])>=upper or float(self.data[i,1])<=lower:
                noise=noise+b",".join(self.data[i])+b" flag: extreme values "+b"\n"
                count_noise+=1
                tic=MPI.Wtime()
                #if batch is filled, write noise to file
                if count_noise>=self.lot_size:
                    self.noise_fh.Write_ordered(noise)
                    count_noise=0
                    noise=b""
                toc=MPI.Wtime()
                self.io_time+=toc-tic
                self.noise_time+=toc-tic
                self.data[i]=["","",""] #mark the data as invalid
        tic=MPI.Wtime()
        if count_noise>0:
            self.noise_fh.Write_ordered(noise)
        toc=MPI.Wtime()
        self.io_time+=toc-tic
        self.noise_time+=toc-tic
    #return io_time used
    def get_io_time(self):
        return self.io_time
    #return time used to write noise to disk
    def get_noise_time(self):
        return self.noise_time
    #return time used to write signal to disk
    def get_sig_time(self):
        return self.sig_time

    '''actually, it does not further remove any data, just copy all left data to signal'''
    def simple_filter(self):
        signal=b""
        count_signal=0
        for i in range(self.n):
            if self.data[i,0]==b"": continue
            else:
                count_signal+=1
                signal=signal+b",".join(self.data[i])+b"\n" #update signal
                tic=MPI.Wtime()
                #if batch is filled, write to file
                if count_signal>=self.lot_size:
                    self.signal_fh.Write_ordered(signal)
                    signal=b""
                    count_signal=0
                toc=MPI.Wtime()
                self.io_time+=toc-tic
                self.sig_time+=toc-tic
        tic=MPI.Wtime()
        if count_signal>0:
            self.signal_fh.Write_ordered(signal)
        toc=MPI.Wtime()
        self.io_time+=toc-tic
        self.sig_time+=toc-tic

    #here is to filter using the method proposed in .r file to maximize autocorrelation with lag 1
    #parameters are determined by using data-small for analysis in R
    def filter_max_corr(self,bar,scrub_block,quota):
        count_data=0
        scrub_arr=numpy.empty(scrub_block,dtype=numpy.uint32) #store batch of data for analysis
        signal=b""
        noise=b""
        count_signal=0
        count_noise=0
        m=0.0
        m2=0.0
        for i in range(self.n):
            if self.data[i,0]==b"":continue
            else:
               scrub_arr[count_data]=i #record the index for a valid data
               temp=float(self.data[i,1])
               count_data+=1
               m+=temp/float(scrub_block) #here is calculating first moment
               m2+=temp**2/float(scrub_block) #here is calculating second moment
               if count_data==scrub_block:#enough valid data collected
                   sd=math.sqrt(m2-m**2) #get standard deviation
                   counter=0 #count the number of outlier in this batch
                   this_noise=b"" #byte arrays to store the potential noise in the batch
                   alter_signal=b"" #bytearrays to store signal
                   total_sig=b"" #bytearrays to store the largest potential signal
                   counter_sig=0
                   marked_index=[] #record indexes for possible noise
                   for j in scrub_arr:
                       total_sig=total_sig+b",".join(self.data[j])+b"\n"
                       if abs(float(self.data[j,1])-m)/sd>=bar: #check if it is outlier
                           counter+=1
                           this_noise=this_noise+b",".join(self.data[j])+b" flag: local outlier "+b"\n"
                           marked_index.append(j)
                       else:
                           alter_signal=alter_signal+b",".join(self.data[j])+b"\n"
                           counter_sig+=1
                   if counter>=quota: #if there are too many outliers
                       count_noise+=counter
                       noise=noise+this_noise #all outliers are regarded as noise
                       count_signal+=counter_sig
                       signal=signal+alter_signal
                       for j in marked_index:
                           self.data[j]=["","",""] #remove confirmed noises
                   else: #if not too many outliers
                       #treat these outliers as part of the normal process
                       count_signal+=scrub_block
                       signal=signal+total_sig
                   tic=MPI.Wtime()
                   #write signal in batch
                   if count_signal>=self.lot_size:
                       self.signal_fh.Write_ordered(signal)
                       count_signal=0
                       signal=b""
                   mid=MPI.Wtime()
                   #write noise in batch
                   if count_noise>=self.lot_size:
                       self.noise_fh.Write_ordered(noise)
                       count_noise=0
                       noise=b""
                   toc=MPI.Wtime()
                   #update time
                   self.io_time+=toc-tic
                   self.noise_time+=toc-mid
                   self.sig_time+=mid-tic
                   count_data=0
                   m=0.0
                   m2=0.0
        tic=MPI.Wtime()
        if count_signal>0:
            self.signal_fh.Write_ordered(signal)
        mid=MPI.Wtime()
        if count_noise>0:
            self.noise_fh.Write_ordered(noise)
        toc=MPI.Wtime()
        self.io_time+=toc-tic
        self.noise_time+=toc-mid
        self.sig_time+=mid-tic
