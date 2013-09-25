package com.objectivity.ig.utility;
import org.slf4j.*;

public class EmailTask extends ConnectTask
{
	
    public EmailTask(String term,long domainId)
    {
    	super(term,domainId);
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
    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
		com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerEmail();
		database.addEdge(baseEdge,domainId,
				 targetVertex.getId(),
				 com.infinitegraph.EdgeKind.OUTGOING,
				 (short)0);	
    }

}   
