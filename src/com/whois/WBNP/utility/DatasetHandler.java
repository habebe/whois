package com.objectivity.ig.utility;
import java.io.*;
import java.util.*;

public class DatasetHandler
{
    private BufferedReader reader = null;
    private int totalCounter = 0;
    private String fileName = null;
    
    public DatasetHandler(String fileName) throws java.io.FileNotFoundException
    {
        this.fileName = fileName;
        InputStream stream = new FileInputStream(fileName);
        this.reader = new BufferedReader(new InputStreamReader(stream));
    }

    public String getFileName()
    {
        return this.fileName;
    }
    
    private DatasetOperation createOperation(com.objectivity.ig.utility.TransactionType transactionType) throws java.io.IOException
					     
    {
        String line = this.reader.readLine();
        DatasetOperation operation = null;
        if(line != null)
        {
            char type = line.charAt(0);
            switch(type)
            {
                case 'A':
                    operation = new WhoisOperation();
                    break;
                case 'B':
                    operation = new WebOperation();
                    break;
                default:
                    System.out.printf("Unknown operation '%c'\n",type);
                    break;
            }
            if(operation != null)
            {
                operation.build(line);
            }
        }
        return operation;
    }
    
    public synchronized List<DatasetOperation> getOperations(int size,com.objectivity.ig.utility.TransactionType transactionType) throws java.io.IOException
    {
        ArrayList<DatasetOperation> result = new ArrayList<DatasetOperation>();
        int counter = 0;
        DatasetOperation operation = null;
        boolean done = false;
        while(!done)
        {
            operation = this.createOperation(transactionType);
            if(operation != null) 
            {
                if(operation.getStatus())
                {
		    result.add(operation);
		    counter += 1;
		}
            }
            done = (operation == null) || (counter >= size);
        }
        totalCounter += counter;
        return result;
    }
}
