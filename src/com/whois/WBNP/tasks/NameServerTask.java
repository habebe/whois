package com.objectivity.ig.utility;
import org.slf4j.*;

public class NameServerTask extends ConnectTask
{
	
    public NameServerTask(String term,long domainId)
    {
    	super(term,domainId);
    }

    @Override
    public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
    {
      targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.NameServer.class, "name");
    }

    @Override
    public void obtainVertexTargets(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
      com.infinitegraph.pipelining.TargetManager targetManager = 
          taskContext.getTargetManager();
      targetVertex = targetManager.getTargetVertex(
    		  com.whois.WBNP.model.vertex.NameServer.class, this.getQueryTerm());
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
