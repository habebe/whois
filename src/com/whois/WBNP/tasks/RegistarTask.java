package com.objectivity.ig.utility;
import org.slf4j.*;

public class RegistarTask extends ConnectTask
{
	
    public RegistarTask(String term,long domainId)
    {
    	super(term,domainId);
    }

    protected void addVertex(com.infinitegraph.GraphDatabase database)
    {
		com.whois.WBNP.model.vertex.Registrar registrar = new com.whois.WBNP.model.vertex.Registrar();
		registrar.set_name(this.getQueryTerm());
		database.addVertex(registrar);
		targetVertex.setId(registrar.getId());
		registrar.updateIndexes();
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
    	com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerRegistrar();
    	database.addEdge(baseEdge,domainId,
			 targetVertex.getId(),
			 com.infinitegraph.EdgeKind.OUTGOING,
			 (short)0);	
    }
}   
