package com.objectivity.ig.utility;

public abstract class DatasetOperation
{
    protected boolean status = false;
    public DatasetOperation()
    {
    }

    public boolean getStatus()
    {
        return this.status;
    }
    
    public int getCounter()
    {
        return 1;
    }

    public abstract int operationType();
    public abstract boolean build(final String statement);
    public abstract long operate(com.objectivity.ig.utility.Operator operator,com.infinitegraph.GraphDatabase database);
}
