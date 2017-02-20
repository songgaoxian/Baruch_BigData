from mpi4py import MPI
import sys
import logging
import cProfile, pstats
from io import StringIO
import psutil
import analyzer
total_start=MPI.Wtime()
on_profile=False #change to true if you need to a logging summary of profile
use_sophisticated_scrub=True #if true, the algo applied will be more sophisticated
#if false, the algo applied will be very simple
pr=None
if on_profile:
    pr=cProfile.Profile() #set up profiling
    pr.enable()
#initialize variables to store time execution
io_time=0
scrub_time=0
write_noise=0
write_sig=0
comm=MPI.COMM_WORLD
rank=comm.Get_rank() #get rank
num_process=comm.Get_size() #get process
logname="./srub_log/srub"+str(rank)+".log"
logging.basicConfig(filename=logname,level=logging.INFO) #config log file for each process
tic=MPI.Wtime()
filename=sys.argv[1] #get file name
#filename="data-small.txt"
file=MPI.File.Open(comm, filename) #open file
toc=MPI.Wtime()
io_time+=toc-tic
file_size=file.Get_size() #get file size
mem=psutil.virtual_memory()
total_usable=mem[7]*0.65 #at most use 65% of available memory, adjustable
#consideration: other variables and functions will also consume memory
remain=int(float(file_size)/num_process) #get total size of data to deal with per process
size_per_process=min(int(float(file_size)/num_process),int(total_usable/num_process))
buffer_per_process=bytearray(size_per_process)
lot_size=max(60000,int(size_per_process/40)) #number of lines to write in batch, adjustable
tic=MPI.Wtime()
noise_f=MPI.File.Open(comm,"noise.txt",4)
signal_f=MPI.File.Open(comm,"signal.txt",4)
logging.info("After open data file, memory usage status: \n"+repr(mem))
toc=MPI.Wtime()
io_time+=toc-tic
up_limit = 4000  # maximum price that is considered as reasonable
low_limit = 0 #min price that is considered as reasonable
bar=1.7 #determined by analysis in R, to be used in scrub algo
scrub_block=900 #determined by analysis in R, to be used in scrub algo
quota=72 #determined by 1.8*scrub_block*(1-pnorm(bar)), analyzed in R
w_start=MPI.Wtime()
while remain>0:
    tic=MPI.Wtime()
    file.Read_ordered(buffer_per_process) #read data to buffer
    toc=MPI.Wtime()
    io_time+=toc-tic
    remain-=size_per_process #update remain
    tic=MPI.Wtime()
    analyze=analyzer.Analyzer(buffer_per_process,lot_size,noise_f,signal_f) #construct analyzer
    toc=MPI.Wtime()
    io_time+=toc-tic
    tic=MPI.Wtime()
    analyze.block_ordering() #do the block ordering
    analyze.filter_extremes(up_limit,low_limit) #filter extreme values
    if use_sophisticated_scrub:
      analyze.filter_max_corr(bar,scrub_block,quota) #apply the scrub algo to maximize autocorrelation
    else:
      analyze.simple_filter() #apply the simple scrub algo
    toc=MPI.Wtime()
    #get execution times
    io_time+=analyze.get_io_time()
    scrub_time+=toc-tic-analyze.get_io_time()
    write_noise+=analyze.get_noise_time()
    write_sig+=analyze.get_sig_time()
tic=MPI.Wtime()
logging.info("While loop takes "+str(tic-w_start))
mem=psutil.virtual_memory()
logging.info("After while loop, memory usage status:\n"+repr(mem))
#file.Sync()
file.Close()
t_noise=MPI.Wtime()
noise_f.Sync()
noise_f.Close()
t_sig=MPI.Wtime()
signal_f.Sync()
signal_f.Close()
toc=MPI.Wtime()
io_time+=toc-tic
write_noise+=t_sig-t_noise
write_sig+=toc-t_sig
#print some info
print("IO uses "+str(io_time))
print("Scrub uses "+str(scrub_time)+" (rank "+str(rank)+")")
print("Write noise to disk uses "+str(write_noise))
print("Write signal to disk uses "+str(write_sig))
if on_profile:
    pr.disable()
    s=StringIO()
    sortby="cumulative"
    ps=pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    logging.info(s.getvalue()) #log the profile of the program
total_end=MPI.Wtime()
print("total time is ",total_end-total_start) #print total execution time