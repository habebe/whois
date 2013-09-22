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
			      new Long(object.getId()));
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
		long time = System.nanoTime();
		/*
		  if(this.vertex.isNeighbor(domainId))
		    {
			java.util.List<com.infinitegraph.EdgeHandle> edges = this.vertex.findEdgesToNeighbor(domainId);
			ipDomainEdge = (com.whois.WBNP.model.edge.IpDomain)edges.get(0).getEdge();
		    }
		*/
		com.infinitegraph.EdgeHandle handle = this.vertex.getEdgeToNeighbor(domainId);
		if(handle != null)
		    ipDomainEdge = (com.whois.WBNP.model.edge.IpDomain)handle.getEdge();
		time = (System.nanoTime()-time);
		int size = this.vertex.getHandle().getEdgeCount();
		logger.info(String.format("C,%d,%d,%d",time,ConnectTask.ProcessCounter,size));
	    }
	/*
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
	*/
    }


    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
	ConnectTask.ProcessCounter += 1;
	long time = System.nanoTime();
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
	gsd.getPlacementWorker().setPolicies(null);
	if(this.vertex == null)
	    {	
		Long entry = this.getDataForTarget(taskContext,
						   getClassName(),
						   getQueryTerm());
		if((entry != null) && (entry > 0))
		    {
 			this.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry));
			this.checkConnectivity();
		    }
		if(this.vertex == null)
		    this.vertex = this.addVertex(taskContext,database);
	    }
	long timeA = (System.nanoTime() - time);
	if(ipDomainEdge == null)
	    {
		this.createConnection(database);
	    }
	else
	    {
		ipDomainEdge.set_volume(ipDomainEdge.get_volume() + this.getVolume());
	    }
	time = (System.nanoTime() - time);
	logger.info(String.format("E,%d,%d",timeA,ConnectTask.ProcessCounter));
	logger.info(String.format("F,%d,%d",time,ConnectTask.ProcessCounter));
    }
}   
