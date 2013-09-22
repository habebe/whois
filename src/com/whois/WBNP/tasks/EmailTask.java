package com.objectivity.ig.utility;
import org.slf4j.*;

public class EmailTask extends ConnectTask
{
	
    public EmailTask(String term,long domainId)
    {
	super(term,domainId);
    }
        
    protected String getClassName()
    {
	return com.whois.WBNP.model.vertex.Email.class.getName();
    }
    
    protected com.infinitegraph.BaseVertex addVertex(com.infinitegraph.pipelining.TaskContext taskContext,
						     com.infinitegraph.GraphDatabase database)
    {
	com.whois.WBNP.model.vertex.Email object = new com.whois.WBNP.model.vertex.Email();
	object.set_name(this.getQueryTerm());
	database.addVertex(object);
	this.setDataForTarget(taskContext,
			      com.whois.WBNP.model.vertex.Email.class.getName(),
			      this.getQueryTerm(),
			      new Long(object.getId()));
	object.updateIndexes();
	return object;
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
	com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerEmail();
	database.addEdge(baseEdge,domainId,
			 this.vertex.getId(),
			 com.infinitegraph.EdgeKind.OUTGOING,
			 (short)0);	
    }

    protected int performQuery(com.infinitegraph.pipelining.TaskContext taskContext,com.infinitegraph.GraphDatabase database)
    {
	this.vertex = this.query(taskContext,database,
				 com.whois.WBNP.model.vertex.Email.class.getName(),
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
				 com.whois.WBNP.model.vertex.Email.class.getName(),
				 this.getQueryTerm(),
				 EmailObjectQualifier
				 );
	if(this.vertex != null)
	    return 1;
	return 0;
    }

}   
