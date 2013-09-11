package com.objectivity.ig.utility;

public class WhoisOperation extends com.objectivity.ig.utility.DatasetOperation
{
    private String domain = null;
    private String email  = null;
    private String registar = null;
    private String nameServer = null;
    private String country = null;

    public WhoisOperation()
    {
    }

    public boolean build(final String statement)
    {
        this.status = false;
	String[] data = statement.split(";");
        if((data != null) && (data.length >= 6))
	    {
		this.status = true;
		domain     = data[1];
		registar   = data[2];
		email      = data[3];
		nameServer = data[4];
		country    = data[5];
		//System.out.printf("D(%s) R(%s) E(%s) N(%s) C(%s)\n",domain,registar,email,nameServer,country);
	    }
	
        return this.status;
    }
    
    public int operationType()
    {
        return 0;
    }
    
    public long operate(com.objectivity.ig.utility.Operator operator,com.infinitegraph.GraphDatabase database)
    {
        long result = 1;
	com.objectivity.ig.utility.WhoisTask task = new com.objectivity.ig.utility.WhoisTask
	    (
		domain,
		country,
		registar,
		email,
		nameServer
	    );
	database.submitPipelineTask(task);
	operator.addOperationCounter(0,1);
	return result;
    }
}
