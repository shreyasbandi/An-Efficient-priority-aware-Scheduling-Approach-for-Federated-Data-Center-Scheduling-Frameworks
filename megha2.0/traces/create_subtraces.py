import random
import sys
trace_name=sys.argv[1]
size=int(sys.argv[2])
number=int(sys.argv[3])

trace_file=open(trace_name).readlines()
trace_size=len(trace_file)

for i in range(0,number):
	offset= random.randint(0,trace_size-size-1)
	subtrace=trace_file[offset:offset+size]
	subtrace_file=open("subtraces/"+trace_name+"_"+str(size)+"_"+str(i)+".txt","w")
	for line in subtrace:
		subtrace_file.write(line)
	
	

