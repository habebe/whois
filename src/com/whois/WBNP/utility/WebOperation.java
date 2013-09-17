package com.objectivity.ig.utility;

public class WebOperation extends com.objectivity.ig.utility.DatasetOperation
{
    private String domain = null;
    private String ip  = null;
    private double volume;

    public WebOperation()
    {
    }

    public boolean build(final String statement)
    {
        this.status = false;
	String[] data = statement.split(";");
        if((data != null) && (data.length >= 3))
	    {
		this.status = true;
		domain     = data[1];
		ip         = data[3];
		volume     = Double.parseDouble(data[2]);
		//System.out.printf("D(%s) R(%s) E(%s) N(%s) C(%s)\n",domain,registar,email,nameServer,country);
	    }
	
        return this.status;
    }
    
    public int operationType()
    {
        return 1;
    }
    
    public long operate(com.objectivity.ig.utility.Operator operator,com.infinitegraph.GraphDatabase database)
    {
        long result = 1;
	com.objectivity.ig.utility.WebTask task = new com.objectivity.ig.utility.WebTask
	    (
		domain,
		ip,
		volume
	    );

	database.submitPipelineTask(task);
	operator.addOperationCounter(1,1);
	return result;
    }
}
