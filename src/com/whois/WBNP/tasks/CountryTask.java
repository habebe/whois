package com.objectivity.ig.utility;
import org.slf4j.*;

public class CountryTask extends ConnectTask
{
	
    public CountryTask(String term,long domainId)
    {
	super(term,domainId);
    }
        
    protected com.infinitegraph.BaseVertex addVertex(com.infinitegraph.pipelining.TaskContext taskContext,
						     com.infinitegraph.GraphDatabase database)
    {
	com.whois.WBNP.model.vertex.Country country = new com.whois.WBNP.model.vertex.Country();
	country.set_name(this.getQueryTerm());
	database.addVertex(country);
	this.setDataForTarget(taskContext,
			      com.whois.WBNP.model.vertex.Country.class.getName(),
			      this.getQueryTerm(),
			      new VertexIDEntry(country.getId()));
	country.updateIndexes();
	return country;
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
	com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerCountry();
	database.addEdge(baseEdge,domainId,
			 this.vertex.getId(),
			 com.infinitegraph.EdgeKind.OUTGOING,
			 (short)0);	
    }

    protected int performQuery(com.infinitegraph.pipelining.TaskContext taskContext,com.infinitegraph.GraphDatabase database)
    {
	this.vertex = this.query(taskContext,database,
				 com.whois.WBNP.model.vertex.Country.class.getName(),
				 String.format("(name == \"%s\"",this.getQueryTerm()));
	if(this.vertex != null)
	    return 1;
	return 0;
    }

 

}   
