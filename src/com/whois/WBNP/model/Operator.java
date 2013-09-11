/********************************************************************************

********************************************************************************/


package com.whois.WBNP.model;
import com.infinitegraph.*;
public class Operator 
{
    private com.infinitegraph.GraphDatabase database = null;
    public Operator(com.infinitegraph.GraphDatabase database)
    {
	this.database = database;
    }
        
    @SuppressWarnings("unchecked")
    public long run
	(
	    com.objectivity.ig.utility.DatasetHandler dataSource,
	    int numberOfThreads,
	    int transactionSize,
	    int transactionLimit,
	    com.objectivity.ig.utility.TransactionType transactionType
	) throws java.lang.Exception
    {
	System.out.printf("Running from file:%s\n",dataSource.getFileName()); 
	int i;
	Thread[] threads = new Thread[numberOfThreads];
	java.util.HashMap<Thread,com.objectivity.ig.utility.Operator> map = new java.util.HashMap<Thread,com.objectivity.ig.utility.Operator>();
	com.objectivity.ig.utility.Profile profile = new com.objectivity.ig.utility.Profile(com.objectivity.ig.utility.Globals.getProfileFileName(),
											    numberOfThreads,transactionSize);
	
	profile.start();
	for(i=0;i<numberOfThreads;i++)
	    {
		
		com.objectivity.ig.utility.ProfileEvent event = new com.objectivity.ig.utility.ProfileEvent(com.objectivity.ig.utility.Globals.getProfileEventFileName());
		com.objectivity.ig.utility.Operator o = new com.objectivity.ig.utility.Operator(this.database,dataSource,transactionSize,
												transactionLimit,transactionType,event);
		threads[i] = new Thread(o);
		map.put(threads[i],o);
		threads[i].start();
	    }
	long counter = 0;
	for(i=0;i<numberOfThreads;i++)
	    {
		threads[i].join();
		com.objectivity.ig.utility.Operator o = map.get(threads[i]);
		counter += o.getCounter();
	    }
	profile.stop();
	
	org.json.simple.JSONObject object = new org.json.simple.JSONObject();
	double rate = counter*1000;
	double time = profile.getElapsedTime();
	rate = rate / time;
	object.put("rate",rate);
	object.put("size",counter);
	int[] operationCounter = new int[com.objectivity.ig.utility.Operator.GetOperationCounterSize()];
	for(i=0;i<com.objectivity.ig.utility.Operator.GetOperationCounterSize();i++)
	    {
		operationCounter[i] = 0;
	    }
	for(i=0;i<numberOfThreads;i++)
	    {
		com.objectivity.ig.utility.Operator o = map.get(threads[i]);
		int[] threadOperationsCounter = o.getOperationCounter();
		for(int j=0;j<com.objectivity.ig.utility.Operator.GetOperationCounterSize();j++)
		    {
			operationCounter[j] += threadOperationsCounter[j];
			
		    }
	    }
	for(int j=0;j<com.objectivity.ig.utility.Operator.GetOperationCounterSize();j++)
	    {
		if(operationCounter[j] > 0)
		    {
			char op = (char)(j + 65);
			object.put(String.format("op.%c",op),operationCounter[j]);
		    }
		
	    }
	profile.save(object);
	if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 0)
	    System.out.println();
	return counter;
    }
}

