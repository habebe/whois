package com.objectivity.ig.utility;
import org.slf4j.*;

public class CountryTask extends ConnectTask
{
    public CountryTask(String term,long domainId)
    {
    	super(term, domainId);
    }
           
    protected void addVertex(com.infinitegraph.GraphDatabase database)
    {
		com.whois.WBNP.model.vertex.Country country = new com.whois.WBNP.model.vertex.Country();
		country.set_name(this.getQueryTerm());
		database.addVertex(country);
		targetVertex.setId(country.getId());
		country.updateIndexes();
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
		com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerCountry();
		database.addEdge(baseEdge,domainId,
				 targetVertex.getId(),
				 com.infinitegraph.EdgeKind.OUTGOING,
				 (short)0);	
    }

}   
