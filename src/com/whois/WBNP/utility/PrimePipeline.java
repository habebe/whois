package com.objectivity.ig.utility;
import com.infinitegraph.*;
import com.infinitegraph.impl.plugins.pwp.pipelining.*;
import com.infinitegraph.impl.*;
import com.objy.db.app.*;
//import com.objy.db.internal.configuration.*;
import org.slf4j.*;


public class PrimePipeline
{
    private static final Logger logger = LoggerFactory.getLogger(PrimePipeline.class);
    private static void primePipeline(GraphDatabase database,String bootFilePath)
    {
	com.infinitegraph.Transaction tx = null;
	
	try
	    {
		int numberSignals = getNumberOfLocations(bootFilePath);
		logger.debug("Priming the pipeline");
		TargetlessTaskSignal[] signals = new TargetlessTaskSignal[numberSignals];
		for(int i=0; i < numberSignals; i++)
		    {
			tx = database.beginTransaction(AccessMode.READ_WRITE);
			TargetlessTaskRoot root = new TargetlessTaskRoot();
			root.persist();
			ooId rootOid = root.getOid();
			long rootId = ObjectivityUtilities.getLongFromOid(rootOid);
			//logger.trace("Targetless Root {} oid is {}", i, rootOid.getString());
			signals[i] = new TargetlessTaskSignal(rootId);
			database.submitPipelineTask(signals[i]);
			tx.commit();              
		    }
	    }
	catch(Exception ex)
	    {
		//System.out.println(ex.toString());
		logger.error("Bad Prime : {}", ex.toString());
	    }
	finally
	    {
		if(tx != null)
		    tx.complete();
	    }
    }

    private static void primePipeline(GraphDatabase database,int size)
    {
        com.infinitegraph.Transaction tx = null;
        try
            {
                int numberSignals = size;
                logger.info(String.format("Priming the pipeline size=%d",size));
                TargetlessTaskSignal[] signals = new TargetlessTaskSignal[numberSignals];
                for(int i=0; i < numberSignals; i++)
                    {
                        tx = database.beginTransaction(AccessMode.READ_WRITE);
                        TargetlessTaskRoot root = new TargetlessTaskRoot();
                        root.persist();
                        ooId rootOid = root.getOid();
                        long rootId = ObjectivityUtilities.getLongFromOid(rootOid);
                        //logger.trace("Targetless Root {} oid is {}", i, rootOid.getString());                                                                            
                        signals[i] = new TargetlessTaskSignal(rootId);
                        database.submitPipelineTask(signals[i]);
                        tx.commit();
                    }
            }
        catch(Exception ex)
            {
                //System.out.println(ex.toString());                                                                                                                       
                logger.error("Bad Prime : {}", ex.toString());
            }
        finally
            {
                if(tx != null)
                    tx.complete();
            }
    }

    
    private static int getNumberOfLocations(String bootFilePath)
    {
	com.objy.db.internal.tools.ToolParameters values = new com.objy.db.internal.tools.ToolParameters();

	values.add(ObjectivityTools.SWITCH_BOOT, bootFilePath);
	String result = ObjectivityTools.executeUTICommand(ObjectivityTools.COMMAND_LIST_STORAGE, values);
	String[] lines = result.split("\r\n|\r|\n");
	System.out.printf("lines %d\n",lines.length);
	int numDefaultLines = 8; // Number of Default Lines in liststorage command
	return (lines.length-numDefaultLines);
    }

    public static void prime(GraphDatabase database)
    {
	String bootFile = Connection.current().getBootFilePath();
	PrimePipeline.primePipeline(database,bootFile);
    }

    public static void prime(GraphDatabase database,int size)
    {
        PrimePipeline.primePipeline(database,size);
    }

}