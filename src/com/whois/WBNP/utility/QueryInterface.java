package com.objectivity.ig.utility;

import com.infinitegraph.GraphDatabase;
import com.infinitegraph.BaseVertex;

public abstract class QueryInterface<T extends com.infinitegraph.BaseVertex> implements com.objectivity.ig.utility.KeylookupInterface
{
    protected String className;
    
    public QueryInterface(String className)
    {
        this.className = className;
    }
    
    public java.util.List<Long> predicateQuery(com.infinitegraph.GraphDatabase database,String predicate,int maxResults)
        throws com.infinitegraph.GraphException
    {
        com.infinitegraph.Query<T> query = database.createQuery(className,predicate);
        java.util.Iterator<T> iterator = query.execute();
        java.util.List<Long> result = new java.util.ArrayList<Long>();
        int counter = 0;
        boolean done = false;
        
        while(iterator.hasNext() && !done)
        {
            result.add(iterator.next().getId());
            if(maxResults > 0)
                done = (counter >= maxResults);
            counter += 1;
        }
        return result;
    }

    public long predicateSingleQuery(com.infinitegraph.GraphDatabase database,String predicate)
        throws com.infinitegraph.GraphException
    {
        com.infinitegraph.Query<T> query = database.createQuery(className,predicate);
        T result = query.getSingleResult();
        if(result != null)
            return result.getId();
        return 0;
    }
}
