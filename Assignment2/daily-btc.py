from blockchain import blockexplorer
from  datetime import datetime
import sys

datestr=sys.argv[1]#get datestring
#datestr="20170220"
date_format=datetime.strptime(datestr,"%Y%m%d")#the converted time starts at 00:00:00
date_format=date_format.replace(hour=23,minute=59,second=59) #adjust it to end of day
time_para=int((date_format-datetime(1970,1,1)).total_seconds()*1000)#get time parameter
blocks=blockexplorer.get_blocks(time=time_para)
unit=100000000  #values are in Satoshish
value=0.0 #store bitcoin traded
count=0
for block in blocks:
   if block.main_chain: #only count main chain
       hash_code=block.hash #get hash code
       theblock=blockexplorer.get_block(hash_code) #get the corresponding block
       tx=theblock.transactions #get transactions in the block
       for t in tx:
            values=t.outputs #get values of outputs
            for item in values:
                   value+=item.value #increment value
            count+=1
            if count%1000==0: print(count,":",value)

total_transactions=count
total_bitcoins=value/float(unit)
print("total transactions is ", total_transactions)
print("total bitcoins are ",total_bitcoins)