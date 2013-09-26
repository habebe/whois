package com.objectivity.ig.utility;
import org.slf4j.*;

public class CountryTask extends ConnectTask
{
    public CountryTask(String term,long domainId)
    {
    	super(term, domainId);
    }
              
    @Override
    public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
    {
      targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Country.class, "name");
    }

    @Override
    public void obtainVertexTargets(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
      com.infinitegraph.pipelining.TargetManager targetManager = 
          taskContext.getTargetManager();
      targetVertex = targetManager.getTargetVertex(
    		  com.whois.WBNP.model.vertex.Country.class, this.getQueryTerm());
    }

    protected void addVertex(com.infinitegraph.GraphDatabase database)
    {
		com.whois.WBNP.model.vertex.Country country = new com.whois.WBNP.model.vertex.Country();
		country.set_name(this.getQueryTerm());
		database.addVertex(country);
		targetVertex.setId(country.getId());
		country.updateIndexes();
    }

    protected void createConnection(
  	      com.infinitegraph.pipelining.TaskContext taskContext)
    {
		com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerCountry();
		taskContext.getGraph().addEdge(baseEdge,domainId,
				 targetVertex.getId(taskContext.getSession()),
				 com.infinitegraph.EdgeKind.OUTGOING,
				 (short)0);	
    }

}   
