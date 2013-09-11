package com.whois.WBNP.model.query;
import com.infinitegraph.*;
import com.infinitegraph.indexing.*;

public class GraphIndex_Ip_ip extends com.objectivity.ig.utility.QueryInterface<com.whois.WBNP.model.vertex.Ip>
{
	public GraphIndex_Ip_ip()
	{
		super(com.whois.WBNP.model.vertex.Ip.class.getName());
	}

  	public void createIndex() throws com.infinitegraph.indexing.IndexException
    	{
     		com.infinitegraph.indexing.IndexManager.addGraphIndex
        	(
                        "GraphIndex_Ip_ip",
			com.whois.WBNP.model.vertex.Ip.class.getName(),
                        new String[] { "ip" },
                        false
        	);
     	}

	public java.util.List<Long> query(com.infinitegraph.GraphDatabase database,int maxResults, String ip)
        throws com.infinitegraph.GraphException
        {
                String predicate = null;
		predicate = String.format(" (ip == \"%s\") ",ip);
                return this.predicateQuery(database,predicate,maxResults);
        }

	public long singleQuery(com.infinitegraph.GraphDatabase database, String ip)
        throws com.infinitegraph.GraphException
        {
                String predicate = null;
		predicate = String.format(" (ip == \"%s\") ",ip);
                return this.predicateSingleQuery(database,predicate);
        }

	public long keyLookup(com.infinitegraph.GraphDatabase database,String key)
	{
		try
		{
			String predicate = null;
			predicate = String.format(" (ip == \"%s\") ",key);
			return this.predicateSingleQuery(database,predicate);
		}
		catch (com.infinitegraph.GraphException e)
		{
			return 0;
		}
	}

	
}

