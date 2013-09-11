/********************************************************************************

********************************************************************************/



package com.whois.WBNP.operations;

import java.io.*;
import com.infinitegraph.*;
import com.infinitegraph.indexing.*;
import org.apache.commons.cli.*;

public class Bootstrap
{
        
    final static public String DefaultPropertFileName = "properties/bootstrap.properties";
    final static public String DefaultGraphName       = "whois";
    private String propertyFileName = null;
    private String graphName = null;
    private GraphDatabase database = null;
    
    private Bootstrap(String name,String propertyFileName)
    {
        this.graphName = name;
        this.propertyFileName = propertyFileName;
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


    public boolean deleteDatabase() throws IOException 
    {
        boolean status = true;
        try
        {
            GraphFactory.delete(graphName,propertyFileName);
            if(com.objectivity.ig.utility.Globals.GetVerboseLevel() > 0)
		System.out.println("\tDeleted graph database whois");
            this.database = null;
        }
        catch (StorageException storageException)
        {
            System.out.println(storageException.getMessage());
            status = false;
        }
        catch (ConfigurationException configurationException)
        {
            System.out.println(configurationException.getMessage());
            status = false;
        }
        return status;
    }
    
    public void createDatabase(boolean createIndex) throws IOException, ConfigurationException
    {
        GraphFactory.create(graphName,propertyFileName);
	System.out.printf("\tCreated graph database use_index:whois %b\n",createIndex);
        Transaction transaction = this.getDatabase().beginTransaction(AccessMode.READ_WRITE);
	if(createIndex == true)
	{        
		try
        	{
            		com.whois.WBNP.model.QueryHandler indexHandler = new com.whois.WBNP.model.QueryHandler();
            		indexHandler.createIndex();
        	}
        	catch (IndexException e)
        	{
            		e.printStackTrace();
        	}
	}
        transaction.commit();
    }

    static private void ShowHelp(Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.printHelp(Bootstrap.class.getName(),options);
    }

    @SuppressWarnings("static-access")
    public static void main(String[] arguments) throws java.io.IOException,org.json.simple.parser.ParseException,com.infinitegraph.ConfigurationException
    {
        Options options = new Options();
        CommandLine commandLine = null;
        String propertyFileName = Bootstrap.DefaultPropertFileName;
        String graphName = Bootstrap.DefaultGraphName;
        final String helpMessage        = "Print this message.";
        final String graphNameMessage   = "Graph database name default=" + graphName;
        final String propertyMessage    = "File path of the property name default=" + propertyFileName;

        options.addOption(new Option("help",helpMessage));
        options.addOption(new Option("overwrite","Overwrite existing database."));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(graphNameMessage).create("graph"));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(propertyMessage).create("property"));
	options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription("verbose level").create("verbose"));

        CommandLineParser parser = new PosixParser();
        try
        {
            commandLine = parser.parse( options, arguments);
	     com.objectivity.ig.utility.Globals.SetVerboseLevel(Integer.parseInt(commandLine.getOptionValue("verbose",(new Integer(0)).toString())));
            if(commandLine.hasOption("help"))
            {
                Bootstrap.ShowHelp(options);
            }
            else
            {
                propertyFileName = commandLine.getOptionValue("property",propertyFileName);
                graphName        = commandLine.getOptionValue("graph",graphName);
                Bootstrap object = new Bootstrap(graphName,propertyFileName);
		


                if(commandLine.hasOption("overwrite"))
                {
                    object.deleteDatabase();
                }
	
                object.createDatabase(true);
            }
        }
        catch (ParseException e)
        {
            System.out.println(e.toString());
            Bootstrap.ShowHelp(options);
        }
       
    }
}

