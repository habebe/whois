/********************************************************************************

********************************************************************************/

 

package com.whois.WBNP.operations;

import java.io.*;
import java.util.*;

import com.infinitegraph.*;
import com.infinitegraph.indexing.*;
import org.apache.commons.cli.*;


public class Benchmark
{
        
    final static public String DefaultPropertFileName = "properties/benchmark.properties";
    final static public String DefaultGraphName       = "whois";


    private GraphDatabase database = null;

    private String graphName = null;
    private String propertyFileName = null;

    private String operationPath = null;
    private String operationFileName = null;	

    private com.objectivity.ig.utility.TransactionType transactionType = com.objectivity.ig.utility.TransactionType.Pipeline;
    private int numberOfThreads = 1;
    private int transactionSize = 10000;
    private int transactionLimit = -1;
    	
    		
    private Benchmark(String name,String propertyFileName)
    {
        this.graphName = name;
        this.propertyFileName = propertyFileName;
    }
    
    private void setOperationFileName(String value)
    {
	this.operationFileName = value;
    }

    private void setNumberOfThreads(int value)
    {
        this.numberOfThreads = value;
    }

    private void setTransactionSize(int value)
    {
        this.transactionSize = value;
    }

    private void setTransactionLimit(int value)
    {
        this.transactionLimit = value;
    }

    private void setTransactionType(com.objectivity.ig.utility.TransactionType value)
    {
	this.transactionType = value;
    }

    private GraphDatabase getDatabase()
    {
        if (database == null) {
            try
            {
                this.database = GraphFactory.open(this.graphName,this.propertyFileName);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
        return this.database;
    }

    private void closeDatabase()
    {
	if(this.database != null)
	{
		this.database.close();
		this.database = null;	
	}	
    }

    public com.objectivity.ig.utility.TransactionType getTransactionType()
    {
	return this.transactionType;
    }

    private void run() throws java.lang.Exception
    {
	java.util.ArrayList<java.io.File> fileList = new ArrayList<java.io.File>();
	if(this.operationFileName != null)
	    {
		fileList.add(new java.io.File(this.operationFileName));		
	    }
	
	if(fileList.size() > 0)
	    {
		
		com.whois.WBNP.model.Operator operator = new com.whois.WBNP.model.Operator(this.getDatabase());
		for(java.io.File file:fileList)
		    {
			if(file.exists())
			    {
				
				operator.run(new com.objectivity.ig.utility.DatasetHandler(file.getPath()),
					     this.numberOfThreads,
					     this.transactionSize,
					     this.transactionLimit,
					     this.transactionType);
				
			    }
		}
	    }
    }
    
    static private void ShowHelp(Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.printHelp(Benchmark.class.getName(),options);
    }
    
    @SuppressWarnings("static-access")
    public static void main(String[] arguments) throws java.lang.Exception,org.json.simple.parser.ParseException,com.infinitegraph.ConfigurationException
    {
        Options options = new Options();
        CommandLine commandLine = null;
       	
        final String helpMessage        = "Print this message.";
        final String graphNameMessage   = "Graph database name default=" + Benchmark.DefaultGraphName;
        final String propertyMessage    = "File path of the property name default=" + Benchmark.DefaultPropertFileName;

	final String opFileNameMessage     = "File path to the operation file.";

	final String threadMessage      = "Number of threads to use.";
        final String txSizeMessage      = "Transaction size.";
        final String txTypeMessage      = "Transaction type (read, write or pipeline).";
        final String txLimitMessage     = "Limit the number of Transactions to a given value. -1 = unlimited.";

        options.addOption(new Option("help",helpMessage));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(graphNameMessage).create("graph"));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(propertyMessage).create("property"));

       
	options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(opFileNameMessage).create("op_file"));
	options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription(threadMessage).create("threads"));

        options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription(txSizeMessage).create("tx_size"));
	options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription(txTypeMessage).create("tx_type"));
	options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription(txLimitMessage).create("tx_limit"));
	options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription("verbose level").create("verbose"));
	options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription("profile tag (<tag>.benchmark.profile").create("profile"));
        CommandLineParser parser = new PosixParser();
        try
        {
            commandLine = parser.parse( options, arguments);
            if(commandLine.hasOption("help"))
            {
                Benchmark.ShowHelp(options);
            }
            else
            {
                
                String graphName          = commandLine.getOptionValue("graph",Benchmark.DefaultGraphName);
		String propertyFileName   = commandLine.getOptionValue("property",Benchmark.DefaultPropertFileName);
		String opFileName = commandLine.getOptionValue("op_file");
		int numberOfThreads = Integer.parseInt(commandLine.getOptionValue("threads",(new Integer(1)).toString()));
		int txSize = Integer.parseInt(commandLine.getOptionValue("tx_size",(new Integer(10000)).toString()));
		int txLimit = Integer.parseInt(commandLine.getOptionValue("tx_limit",(new Integer(-1)).toString()));
		com.objectivity.ig.utility.Globals.SetVerboseLevel(Integer.parseInt(commandLine.getOptionValue("verbose",(new Integer(2)).toString())));
		com.objectivity.ig.utility.Globals.setProfileTag(commandLine.getOptionValue("profile"));

                if(opFileName == null)
                {
			System.out.println("Error: op_file is not given.");
                    	Benchmark.ShowHelp(options);   
		}
		else
		{
                    	Benchmark object = new Benchmark(graphName,propertyFileName);
                    	object.setNumberOfThreads(numberOfThreads);
	                object.setTransactionSize(txSize);
			object.setTransactionLimit(txLimit);
			object.setOperationFileName(opFileName);
			String tx_type  = commandLine.getOptionValue("tx_type","pipeline");
			if(tx_type != null)
			{
				if(tx_type.equalsIgnoreCase("read"))
				{
					object.setTransactionType(com.objectivity.ig.utility.TransactionType.Read);
					if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 0)	
						System.out.println("Running in read mode.");	
				}
				else if(tx_type.equalsIgnoreCase("write"))
				{
					object.setTransactionType(com.objectivity.ig.utility.TransactionType.Write);
					if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 0)	
						System.out.println("Running in write mode.");
					com.objectivity.ig.utility.PrimePipeline.prime(object.getDatabase());
				}
				else if(tx_type.equalsIgnoreCase("pipeline"))
				{
					object.setTransactionType(com.objectivity.ig.utility.TransactionType.Pipeline);
					if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 0)
						System.out.println("Running in write/pipeline mode.");
					com.objectivity.ig.utility.PrimePipeline.prime(object.getDatabase());
				}
				else
				{
					System.out.printf("Unknown transaction type '%s' Use (read,write,pipeline)\n",tx_type);
					object.closeDatabase();
					return;
				}				
			}
			object.run();
		    	object.closeDatabase();
		}	
                
            }
        }
        catch (ParseException e)
        {   
            System.out.println(e.toString());
            Benchmark.ShowHelp(options);
        }
       
    }
}  

