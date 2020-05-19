
# Counting the number of tickets per street segment in New York City 2015-2019.


The *final_challenge.py* file counts the number fo tickets per each street 
segment in NYC. 
The name of the files to be processed are encoded within the code.

To run *final_challenge.py* in Dumbo cluster we must use the following script:

`spark-submit --num-executors ? --executor-cores ? --executor-memory ? final_challenge.py`

Running this code with 30 workers took 5 minutes and 3 seconds. 
It counted approximately 9 milions tickets total. 
Below is the script used to run the code.

`spark-submit --num-executors 6 --executor-cores 5 --executor-memory 10G final_challenge.py`
             
The final output of the code is folder named *nyc_tickets_count*. 
