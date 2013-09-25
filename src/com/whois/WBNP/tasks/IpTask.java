package com.objectivity.ig.utility;
import org.slf4j.*;

import com.infinitegraph.BaseVertex;
import com.infinitegraph.impl.ObjectivityUtilities;

public class IpTask extends com.infinitegraph.pipelining.QueryTask
{
    private double volume;
    private long domainId = 0;
    private transient com.whois.WBNP.model.edge.IpDomain ipDomainEdge = null;
    private transient com.infinitegraph.pipelining.TargetVertex ipTargetVertex;
    private static final Logger logger = LoggerFactory.getLogger(IpTask.class);

    public IpTask(String term,long domainId,double volume)
    {
		super(term);
		this.domainId = domainId;
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
    
    @Override
    public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
    {
      targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Ip.class, "ip");
    }

    @Override
    public void obtainVertexTargets(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
      com.infinitegraph.pipelining.TargetManager targetManager = 
          taskContext.getTargetManager();
      this.ipTargetVertex = targetManager.getTargetVertex(
          com.whois.WBNP.model.vertex.Ip.class, this.getQueryTerm());
    }

    protected void addVertex(com.infinitegraph.GraphDatabase database)
    {
		com.whois.WBNP.model.vertex.Ip ipVertex = new com.whois.WBNP.model.vertex.Ip();
		ipVertex.set_ip(this.getQueryTerm());
		database.addVertex(ipVertex);
		ipTargetVertex.setId(ipVertex.getId());
		ipVertex.updateIndexes();
    }

    protected void createConnection(com.infinitegraph.GraphDatabase database)
    {
    	ipDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
		ipDomainEdge.set_volume(this.getVolume());
		long ipId = this.ipTargetVertex.getId();
		if(domainId < ipId)
		    database.addEdge(ipDomainEdge,domainId,ipId,
				     com.infinitegraph.EdgeKind.OUTGOING,
				     (short)0);
		else
		    database.addEdge(ipDomainEdge,ipId,domainId,
				     com.infinitegraph.EdgeKind.INCOMING,
				     (short)0);
    }


    @Override
	public void checkConnectivity(com.infinitegraph.pipelining.TaskContext taskContext)
    {
		if (ipTargetVertex.wasFound()) {
			long time = System.nanoTime();
	   		BaseVertex vertexObj = (BaseVertex) ObjectivityUtilities
	   	            .getObjectFromLong(taskContext.getSession(),
	   	                ipTargetVertex.getId());
	   	 
			com.infinitegraph.EdgeHandle handle = vertexObj.getEdgeToNeighbor(domainId);
			if (handle != null)
				ipDomainEdge = (com.whois.WBNP.model.edge.IpDomain) handle
						.getEdge();
			time = (System.nanoTime() - time);
			int size = vertexObj.getHandle().getEdgeCount();
			logger.info(String.format("C,%d,%d,%d", time,
					ConnectTask.ProcessCounter, size));
		}
    }


    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
		ConnectTask.ProcessCounter += 1;
		long time = System.nanoTime();
		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
				.getSessionData(taskContext.getSession());
		gsd.getPlacementWorker().setPolicies(null);
		
		if (ipTargetVertex.requiresCreation()) {
			// if we didn't create it within this batch, we probably need to query for it.
			obtainVertexTargets(taskContext);
			if (ipTargetVertex.requiresCreation()) {
				this.addVertex(database);
			}
		}
		long timeA = (System.nanoTime() - time);
		if (ipDomainEdge == null) {
			this.createConnection(database);
		} else {
			ipDomainEdge.set_volume(ipDomainEdge.get_volume()
					+ this.getVolume());
		}
		time = (System.nanoTime() - time);
		logger.info(String.format("E,%d,%d", timeA, ConnectTask.ProcessCounter));
		logger.info(String.format("F,%d,%d", time, ConnectTask.ProcessCounter));
    }

    @Override
    public String getAuditLogEntry()
    {
      return "IpTask";
    }
}   
