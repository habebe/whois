package com.objectivity.ig.utility;
import org.slf4j.*;

public class NameServerTask extends ConnectTask
{
	
    public NameServerTask(String term,long domainId)
    {
    	super(term,domainId);
    }
    
    @Override
    protected void addVertex(com.infinitegraph.GraphDatabase database)
    {
		com.whois.WBNP.model.vertex.NameServer nameServer = new com.whois.WBNP.model.vertex.NameServer();
		nameServer.set_name(this.getQueryTerm());
		database.addVertex(nameServer);
		targetVertex.setId(nameServer.getId());		
		nameServer.updateIndexes();
    }

    @Override
    protected void createConnection(
  	      com.infinitegraph.pipelining.TaskContext taskContext)
    {
		com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.NameServerEdge();
		taskContext.getGraph().addEdge(baseEdge,domainId,
				 targetVertex.getId(taskContext.getSession()),
				 com.infinitegraph.EdgeKind.OUTGOING,
				 (short)0);	
    }

}   
