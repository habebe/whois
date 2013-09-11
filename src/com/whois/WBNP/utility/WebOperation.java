package com.objectivity.ig.utility;

public class WebOperation extends com.objectivity.ig.utility.DatasetOperation
{
    private String[] data = null;
    public WebOperation()
    {
    }

    public boolean build(final String statement)
    {
        this.status = false;
        this.data = statement.split(";");
        if((this.data != null) && (this.data.length >= 2))
	    {
		this.status = true;
	    }
        return this.status;
    }
    
    public int operationType()
    {
        return 0;
    }
    
    public long operate(com.objectivity.ig.utility.Operator operator,com.infinitegraph.GraphDatabase database)
    {
        long result = 0;
	return result;
    }
}
