package com.objectivity.ig.utility;
import org.slf4j.*;

public class EmailTask extends ConnectTask
{
	
    public EmailTask(String term,long domainId)
    {
    	super(term,domainId);
    }
 
    @Override
    public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
    {
      targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Email.class, "name");
    }

    @Override
    public void obtainVertexTargets(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
      com.infinitegraph.pipelining.TargetManager targetManager = 
          taskContext.getTargetManager();
      targetVertex = targetManager.getTargetVertex(
    		  com.whois.WBNP.model.vertex.Email.class, this.getQueryTerm());
    }


    @Override
    protected void addVertex(com.infinitegraph.GraphDatabase database)
    {
		com.whois.WBNP.model.vertex.Email email = new com.whois.WBNP.model.vertex.Email();
		email.set_name(this.getQueryTerm());
		database.addVertex(email);
		targetVertex.setId(email.getId());
		email.updateIndexes();
    }

    @Override
    protected void createConnection(
  	      com.infinitegraph.pipelining.TaskContext taskContext)
    {
		com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerEmail();
		taskContext.getGraph().addEdge(baseEdge,domainId,
				 targetVertex.getId(taskContext.getSession()),
				 com.infinitegraph.EdgeKind.OUTGOING,
				 (short)0);	
    }

}   
