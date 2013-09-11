package com.objectivity.ig.utility;
import org.slf4j.*;

public class IpTask extends ConnectTask
{
    private double volume;
    private transient com.whois.WBNP.model.edge.IpDomain ipDomainEdge = null;
    public IpTask(String term,long domainId,double volume)
    {
	super(term,domainId);
	this.setVolume(volume);
    }
        
    void setVolume(double volume)
    {
	markModified();
	this.volume = volume;
    }

    double getVolume()
    {
	fetch();
	return this.volume;
    }

    protected String getClassName()
    {
	return com.whois.WBNP.model.vertex.Ip.class.getName();
    }

    protected com.infinitegraph.BaseVertex addVertex(com.infinitegraph.pipelining.TaskContext taskContext,
						     com.infinitegraph.GraphDatabase database)
    {
	com.whois.WBNP.model.vertex.Ip object = new com.whois.WBNP.model.vertex.Ip();
	object.set_ip(this.getQueryTerm());
	database.addVertex(object);
	this.setDataForTarget(taskContext,
			      com.whois.WBNP.model.vertex.Ip.class.getName(),
			      this.getQueryTerm(),
			      new VertexIDEntry(object.getId()));
	object.updateIndexes();
	return object;
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
	com.whois.WBNP.model.edge.IpDomain ipDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
	ipDomainEdge.set_volume(this.getVolume());
	database.addEdge(ipDomainEdge,domainId,
			 this.vertex.getId(),
			 com.infinitegraph.EdgeKind.OUTGOING,
			 (short)0);	
    }

    protected int performQuery(com.infinitegraph.pipelining.TaskContext taskContext,com.infinitegraph.GraphDatabase database)
    {
	this.vertex = this.query(taskContext,database,
				 com.whois.WBNP.model.vertex.Ip.class.getName(),
				 String.format("(ip == \"%s\")",this.getQueryTerm()));
	if(this.vertex != null)
	    return 1;
	return 0;
    }

    protected int performQueryUsingQualifier(com.infinitegraph.pipelining.TaskContext taskContext,
					     com.infinitegraph.GraphDatabase database)
    {
	this.initializeQualifiers();
	this.vertex = this.query(taskContext,database,
				 com.whois.WBNP.model.vertex.Ip.class.getName(),
				 this.getQueryTerm(),
				 IpObjectQualifier
				 );
	if(this.vertex != null)
	    return 1;
	return 0;
    }

    protected void checkConnectivity()
    {
	if(this.vertex != null)
	    {
		
		for(com.infinitegraph.EdgeHandle edgeHandle : this.vertex.getEdges())
		    {
			com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
			if(domainId == vertexHandle.getId())
			    {
				
				ipDomainEdge = (com.whois.WBNP.model.edge.IpDomain)edgeHandle.getEdge();
				return;
			    }
		    }
	    }
    }


    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
	ConnectTask.ProcessCounter += 1;
	logger.info(String.format("C,2,%d,%d",System.currentTimeMillis(),ConnectTask.ProcessCounter));
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
	gsd.getPlacementWorker().setPolicies(null);
	if(this.vertex == null)
	    {	
		VertexIDEntry entry = this.getDataForTarget(taskContext,
							    getClassName(),
							    getQueryTerm());
		if((entry != null) && (entry.id > 0))
		    {
			this.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.id));
			this.checkConnectivity();
		    }
		if(this.vertex == null)
		    this.vertex = this.addVertex(taskContext,database);
	    }
	if(ipDomainEdge == null)
	    {
		this.createConnection(database);
	    }
	else
	    {
		ipDomainEdge.set_volume(ipDomainEdge.get_volume() + this.getVolume());
	    }
	logger.info(String.format("C,3,%d,%d",System.currentTimeMillis(),ConnectTask.ProcessCounter));
    }
}   
