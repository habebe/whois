package com.objectivity.ig.utility;
import org.slf4j.*;

import com.infinitegraph.BaseVertex;
import com.infinitegraph.VertexHandle;
import com.infinitegraph.impl.ObjectivityUtilities;

public class IpTask extends com.infinitegraph.pipelining.QueryTask
{
    private double volume;
    private long domainId = 0;
    
    private transient com.whois.WBNP.model.edge.IpDomain ipDomainEdge = null;
    private transient com.infinitegraph.pipelining.TargetVertex ipTargetVertex;
    
    private static final Logger logger = LoggerFactory.getLogger(IpTask.class);

    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		


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

    protected void createConnection(com.infinitegraph.pipelining.TaskContext taskContext)
    {
    	ipDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
		ipDomainEdge.set_volume(this.getVolume());
		long ipId = this.ipTargetVertex.getId(taskContext.getSession());
		//logger.info(String.format(">> CC,%d,%d", ipId, domainId));
		if(domainId < ipId)
		    taskContext.getGraph().addEdge(ipDomainEdge,domainId,ipId,
				     com.infinitegraph.EdgeKind.OUTGOING,
				     (short)0);
		else
		    taskContext.getGraph().addEdge(ipDomainEdge,ipId,domainId,
				     com.infinitegraph.EdgeKind.INCOMING,
				     (short)0);
		//logger.info(" << CC");
    }


    @Override
	public void checkConnectivity(com.infinitegraph.pipelining.TaskContext taskContext)
    {
		if (ipTargetVertex.wasFound()) {
			long time = System.nanoTime();
			VertexHandle vertexHandle = taskContext.getGraph().getVertexHandle(
					ipTargetVertex.getId(taskContext.getSession()));
	   		BaseVertex vertexObj = (BaseVertex) vertexHandle.getVertex();
	   	 
	   		// TODO - we need to get the getEdgeToNeighbor() on the VertexHandle.
			com.infinitegraph.EdgeHandle handle = vertexObj.getEdgeToNeighbor(domainId);
			if (handle != null)
				ipDomainEdge = (com.whois.WBNP.model.edge.IpDomain) handle
						.getEdge();
			time = (System.nanoTime() - time);
			int size = vertexHandle.getEdgeCount();
			logger.info(String.format("C,%d,%d,%d", time,
					ProcessCounter, size));
		}
    }


    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {   
    	//logger.info(">> E <<");
		ProcessCounter += 1;
		long time = System.nanoTime();
		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
				.getSessionData(taskContext.getSession());
		gsd.getPlacementWorker().setPolicies(null);
		
		if (ipTargetVertex.requiresCreation()) {
			this.addVertex(database);
		}
		//logger.info(">> F <<");
		long timeA = (System.nanoTime() - time);
		if (ipDomainEdge == null) {
			this.createConnection(taskContext);
		} else {
			ipDomainEdge.set_volume(ipDomainEdge.get_volume()
					+ this.getVolume());
		}
		time = (System.nanoTime() - time);
		logger.info(String.format("E,%d,%d", timeA, ProcessCounter));
		logger.info(String.format("F,%d,%d", time, ProcessCounter));
    }

    @Override
    public String getAuditLogEntry()
    {
      return "IpTask";
    }
}   
