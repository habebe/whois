package com.objectivity.ig.utility;

import java.util.concurrent.*;
import com.infinitegraph.*;
import org.json.simple.*;

public class Operator implements Runnable
{
    private com.infinitegraph.GraphDatabase database;
    private DatasetHandler dataSource = null;
    private int transactionSize = 10000;
    private int transactionLimit = -1;
    private com.objectivity.ig.utility.TransactionType transactionType = com.objectivity.ig.utility.TransactionType.Write;
    private int counter = 0;
    private ProfileEvent event = null;
    private int numberOfTransactions = 0;
    private int[] operationsCounter = new int[26];
    private int[] totalOperationsCounter = new int[26];
    
    public Operator(com.infinitegraph.GraphDatabase database,
                    DatasetHandler dataSource,
		    int transactionSize,
                    int transactionLimit,
                    com.objectivity.ig.utility.TransactionType transactionType,
                    ProfileEvent event
                    )
    {
        this.database = database;
        this.dataSource = dataSource;
	this.transactionSize = transactionSize;
        this.transactionLimit = transactionLimit;
        this.transactionType = transactionType;
        this.event = event;
        this.resetOperationCounter();
        this.resetTotalOperationCounter();
    }
    
    private void resetOperationCounter()
    {
        for(int i=0;i<26;i++)
            operationsCounter[i] = 0;
    }

    private void resetTotalOperationCounter()
    {
        for(int i=0;i<26;i++)
            totalOperationsCounter[i] = 0;
    }

    public void addOperationCounter(int index,int value)
    {
        operationsCounter[index] += value;
    }

    private void addToTotalOperationCounter()
    {
      for(int i=0;i<26;i++)
	  {
	      totalOperationsCounter[i] += operationsCounter[i];
	  }
    }
    
    public int getCounter()
    {
        return this.counter;
    }

    public final int[] getOperationCounter()
    {
        return totalOperationsCounter;
    }

    public static int GetOperationCounterSize()
    {
        return 26;
    }
    
    private void startEvent()
    {
        if(this.event != null)
            this.event.start();
    }

    @SuppressWarnings("unchecked")
    private void stopEvent() throws java.lang.Exception
    {
        if(this.event != null)
        {
            this.event.stop();
            JSONObject object = new JSONObject();
            object.put("id",Thread.currentThread().getId());
            object.put("counter",this.counter);
            int opCounter = 0;
	    if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 1)
		{
		    System.out.println("-------------------------------------------------------------------------");
		}
	   	    
            for(int i=0;i<26;i++)
            {
                if(operationsCounter[i] > 0)
                {
                    char op = (char)(i + 65);
                    object.put(op,operationsCounter[i]);
		    opCounter += operationsCounter[i];
		    if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 1)
			{
			    switch(op)
				{
				case 'A':
				    System.out.printf("\twhois upsert : %d\n",operationsCounter[i]);
				    break;
				case 'B':
				    System.out.printf("\tweb upsert : %d\n",operationsCounter[i]);
				    break;
				default: 
				    System.out.printf("\t%c : %d\n",op,operationsCounter[i]);
				}
			    
			}
		}
	    }
	    double rate = opCounter*1000;
            double time = this.event.getElapsedTime();
            rate = rate/time;
	    if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 1)
		{
		    System.out.printf("\tTotal operations : %d\n",opCounter);
		    System.out.printf("\tTime             : %f seconds\n",this.event.getElapsedTime()/1000.0);
		    System.out.printf("\tRate (ops/sec)   : %f\n",rate);
		}
	    switch(this.transactionType)
            {
                case Write:
                    object.put("Transaction","write");
                    break;
                case Read:
                    object.put("Transaction","read");
                    break;
                case Pipeline:
                    object.put("Transaction","pipeline");
                    break;
            }
            
            object.put("rate",rate);
            this.event.save(object);
        }
    }


    private Transaction beginTransaction()
    {
        Transaction transaction = null;
        switch(this.transactionType)
        {
            case Write:
                System.out.println("WRITE Tx");
                transaction = this.database.beginTransaction(AccessMode.READ_WRITE);
                break;
            case Read:
                System.out.println("READ Tx");
                transaction = this.database.beginTransaction(AccessMode.READ);
                break;
            case Pipeline:
                System.out.println("PIPELINE Tx");
                transaction = this.database.beginTransaction(AccessMode.READ_WRITE,
                                                             new com.infinitegraph.policies.PolicyChain(new com.infinitegraph.policies.EdgePipeliningPolicy()));
                break;

        }
        this.numberOfTransactions++;
        return transaction;
    }

    private boolean isTransactionLimitReached()
    {
        if(this.transactionLimit <= 0)
            return false;
        return (this.transactionLimit <= this.numberOfTransactions);
    }
    
    public void run()
    {
        Transaction transaction = null;
        try
        {
            java.util.List<DatasetOperation> operations = this.dataSource.getOperations(this.transactionSize,this.transactionType);
            while((operations != null) && (operations.size() > 0) && (!this.isTransactionLimitReached()))
            {
                this.resetOperationCounter();
		int numberOfRetries = 0;
                boolean complete = false;
		this.startEvent();
		while(!complete)
		    {
			try
			    {
				transaction = this.beginTransaction();
				java.util.Iterator<DatasetOperation> iterator = operations.iterator();
				while(iterator.hasNext())
				    {
					DatasetOperation operation = iterator.next();
					if(operation.operate(this,this.database) > 0)
					    this.counter += 1;
				    }
				transaction.commit();
				complete = true;
			    }
			catch(com.infinitegraph.StorageException storageException)
			    {
				System.out.printf("STORAGE EXCEPTION must ABORT and retry. counter=%d exception=%s\n",numberOfRetries,storageException.toString());
				transaction.complete();
				numberOfRetries += 1;
				complete = (numberOfRetries >= 3);
				this.resetOperationCounter();
				if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 0)
				    storageException.printStackTrace();
			    }
		    }
		this.stopEvent();
                this.addToTotalOperationCounter();
		if(com.objectivity.ig.utility.Globals.GetVerboseLevel() == 1)
		    System.out.printf(".");
                operations  = this.dataSource.getOperations(this.transactionSize,this.transactionType);
            }
	    if(com.objectivity.ig.utility.Globals.GetVerboseLevel() == 1)
		System.out.printf("x (%d)",this.counter);
        }
        catch(java.lang.Exception exception)
        {
            exception.printStackTrace();
        }
    }
}
