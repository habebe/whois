package com.whois.WBNP.model.query;
import com.infinitegraph.*;
import com.infinitegraph.indexing.*;

public class GraphIndex_Domain_name extends com.objectivity.ig.utility.QueryInterface<com.whois.WBNP.model.vertex.Domain>
{
	public GraphIndex_Domain_name()
	{
		super(com.whois.WBNP.model.vertex.Domain.class.getName());
	}

  	public void createIndex() throws com.infinitegraph.indexing.IndexException
    	{
     		com.infinitegraph.indexing.IndexManager.addGraphIndex
        	(
                        "GraphIndex_Domain_name",
			com.whois.WBNP.model.vertex.Domain.class.getName(),
                        new String[] { "name" },
                        false
        	);
     	}

	public java.util.List<Long> query(com.infinitegraph.GraphDatabase database,int maxResults, String name)
        throws com.infinitegraph.GraphException
        {
                String predicate = null;
		predicate = String.format(" (name == \"%s\") ",name);
                return this.predicateQuery(database,predicate,maxResults);
        }

	public long singleQuery(com.infinitegraph.GraphDatabase database, String name)
        throws com.infinitegraph.GraphException
        {
                String predicate = null;
		predicate = String.format(" (name == \"%s\") ",name);
                return this.predicateSingleQuery(database,predicate);
        }

	public long keyLookup(com.infinitegraph.GraphDatabase database,String key)
	{
		try
		{
			String predicate = null;
			predicate = String.format(" (name == \"%s\") ",key);
			return this.predicateSingleQuery(database,predicate);
		}
		catch (com.infinitegraph.GraphException e)
		{
			return 0;
		}
	}

	
}

