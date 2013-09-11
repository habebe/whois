package com.objectivity.ig.utility;
import org.slf4j.*;

public class RegistarTask extends ConnectTask
{
	
    public RegistarTask(String term,long domainId)
    {
	super(term,domainId);
    }

    protected String getClassName()
    {
	return com.whois.WBNP.model.vertex.Registrar.class.getName();
    }
        
    protected com.infinitegraph.BaseVertex addVertex(com.infinitegraph.pipelining.TaskContext taskContext,
						     com.infinitegraph.GraphDatabase database)
    {
	com.whois.WBNP.model.vertex.Registrar object = new com.whois.WBNP.model.vertex.Registrar();
	object.set_name(this.getQueryTerm());
	database.addVertex(object);
	this.setDataForTarget(taskContext,
			      com.whois.WBNP.model.vertex.Registrar.class.getName(),
			      this.getQueryTerm(),
			      new VertexIDEntry(object.getId()));
	object.updateIndexes();
	return object;
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
	com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerRegistrar();
	database.addEdge(baseEdge,domainId,
			 this.vertex.getId(),
			 com.infinitegraph.EdgeKind.OUTGOING,
			 (short)0);	
    }

    protected int performQuery(com.infinitegraph.pipelining.TaskContext taskContext,com.infinitegraph.GraphDatabase database)
    {
	this.vertex = this.query(taskContext,database,
				 com.whois.WBNP.model.vertex.Registrar.class.getName(),
				 String.format("(name == \"%s\")",this.getQueryTerm()));
	if(this.vertex != null)
	    return 1;
	return 0;
    }

    
    protected int performQueryUsingQualifier(com.infinitegraph.pipelining.TaskContext taskContext,
					     com.infinitegraph.GraphDatabase database)
    {
	this.initializeQualifiers();
	this.vertex = this.query(taskContext,database,
				 com.whois.WBNP.model.vertex.Registrar.class.getName(),
				 this.getQueryTerm(),
				 RegistrarObjectQualifier
				 );
	if(this.vertex != null)
	    return 1;
	return 0;
    }
}   
