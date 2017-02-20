from mpi4py import MPI
import logging
import cProfile, pstats
from io import StringIO
import psutil
import numpy
import sys
import normal
#similar setting as scrub
start=MPI.Wtime()
on_profile=False #change to true if you need to a logging summary of profile; otherwise, make it false
pr=None
if on_profile:
    pr=cProfile.Profile()
    pr.enable()
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
num_process=comm.Get_size()
logname="./normal_log/normal"+str(rank)+".log"
logging.basicConfig(filename=logname,level=logging.INFO)
filename=sys.argv[1] #contain time series data
tic=MPI.Wtime()
file=MPI.File.Open(comm, filename)
file_size=file.Get_size()
toc=MPI.Wtime()
mem=psutil.virtual_memory()
logging.info("current memory usage after opening data file: \n"+repr(mem))
logging.info("open file uses "+str(toc-tic))
total_usable=mem[7]*0.65 #at most use 65%, adjustable
#consideration: other variables and functions will also consume memory
remain=int(float(file_size)/num_process)
size_per_process=min(int(float(file_size)/num_process),int(total_usable/num_process))
buffer_per_process=bytearray(size_per_process)
#initialize local and global variables for statistical analysis
local_sample_size=numpy.zeros(1)
global_sample_size=numpy.zeros(1)
local_first_moment=numpy.zeros(1)
global_first_moment=numpy.zeros(1)
local_second_moment=numpy.zeros(1)
global_second_moment=numpy.zeros(1)
local_third_moment=numpy.zeros(1)
global_third_moment=numpy.zeros(1)
local_forth_moment=numpy.zeros(1)
global_forth_moment=numpy.zeros(1)
tic=MPI.Wtime()
while remain>0:
    file.Read_ordered(buffer_per_process)
    remain-=size_per_process
    #create Normal_stat
    norm_test=normal.Normal_stat(buffer_per_process,True) #if the passed time sereis is return, set False
    #set moments and sample size for time series return
    stat=norm_test.moments_and_size()
    local_sample_size[0]+=stat[0]
    local_first_moment[0]+=stat[1]*stat[0]
    local_second_moment[0]+=stat[2]*stat[0]
    local_third_moment[0]+=stat[3]*stat[0]
    local_forth_moment[0]+=stat[4]*stat[0]
toc=MPI.Wtime()
logging.info("The while loop for parsing bytearray and normal test takes "+str(toc-tic))
mem=psutil.virtual_memory()
logging.info("After the while loop, current memory usage:\n"+repr(mem))
tic=MPI.Wtime()
#sum all local sample size to global sample size
comm.Allreduce(local_sample_size,global_sample_size,op=MPI.SUM)
local_first_moment[0]/=global_sample_size[0]
local_second_moment[0]/=global_sample_size[0]
local_third_moment[0]/=global_sample_size[0]
local_forth_moment[0]/=global_sample_size[0]
#calculate first to forth moments
comm.Reduce(local_first_moment,global_first_moment,op=MPI.SUM)
comm.Reduce(local_second_moment,global_second_moment,op=MPI.SUM)
comm.Reduce(local_third_moment,global_third_moment,op=MPI.SUM)
comm.Reduce(local_forth_moment,global_forth_moment,op=MPI.SUM)
toc=MPI.Wtime()
logging.info("Data communication takes "+str(toc-tic))
if rank==0:
    #calculate statistics
    skew=normal.getSkew(global_first_moment[0],global_second_moment[0],global_third_moment[0])
    kurt=normal.getKurtosis(global_first_moment[0],global_second_moment[0],global_third_moment[0],global_forth_moment[0])
    p_val=normal.getJarque_Bera_test_pval(global_sample_size[0],skew,kurt)
    print("p value for this normality test (Jarque_Bera test) for this set of time series is ",p_val)
    print("mean is ",global_first_moment[0])
    print("variance is ",global_second_moment[0]-global_first_moment[0]**2)
    print("kurtosis is ",kurt)
    print("skewness is ",skew)
    #significance level is 0.05
    if p_val>0.05:
        print("normality is accepted")
    else:
        print("normality is rejected")
#file.Sync()
file.Close()
if on_profile:
    pr.disable()
    s=StringIO()
    sortby="cumulative"
    ps=pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    logging.info(s.getvalue())
theend=MPI.Wtime()
print("the normal test takes ",theend-start)