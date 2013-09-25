package com.objectivity.ig.utility;
import org.slf4j.*;

import com.infinitegraph.BaseVertex;
import com.infinitegraph.impl.ObjectivityUtilities;

public abstract class ConnectTask extends com.infinitegraph.pipelining.QueryTask
{
    protected static final Logger logger = LoggerFactory.getLogger(ConnectTask.class);
    protected long domainId = 0;
    private transient long domainEdgeId = 0;
    protected transient com.infinitegraph.pipelining.TargetVertex targetVertex = null;
    
    public ConnectTask(String term,long domainId)
    {
    	super(term);
    	this.set(domainId);
    }
    
    private void set(long domainId)
    {
    	this.domainId = domainId;
    	this.markModified();
    }

    protected long getDomainId()
    {
    	fetch();
    	return this.domainId;
    }
    
    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		

    protected abstract void addVertex(com.infinitegraph.GraphDatabase database);
    protected abstract void createConnection(com.infinitegraph.GraphDatabase database);
    
    @Override
    public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
    {
      targetManager.setPrimaryKey(this.getClass(), "name");
    }

    @Override
    public void obtainVertexTargets(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
      com.infinitegraph.pipelining.TargetManager targetManager = 
          taskContext.getTargetManager();
      targetVertex = targetManager.getTargetVertex(this.getClass(), this.getQueryTerm());
    }
    
    
    @Override
    public void checkConnectivity(
    	      com.infinitegraph.pipelining.TaskContext taskContext)
    {
    	if(targetVertex.wasFound())
	    {
    		long time = System.nanoTime();
    		BaseVertex vertexObj = (BaseVertex) ObjectivityUtilities
            .getObjectFromLong(taskContext.getSession(),
                targetVertex.getId());
    		com.infinitegraph.EdgeHandle handle = vertexObj.getEdgeToNeighbor(this.domainId);
    	    if (handle != null)
    	        domainEdgeId = handle.getEdge().getId();	
    		time = (System.nanoTime() - time);
    		int size = vertexObj.getHandle().getEdgeCount();
    		logger.info(String.format("C,%d,%d,%d,%s,%b",time,ConnectTask.ProcessCounter,size,this.getQueryTerm(),(domainEdgeId != 0)));
	    }
    }

    @Override
    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
 {
		ConnectTask.ProcessCounter += 1;
		long time = System.nanoTime();
		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
				.getSessionData(taskContext.getSession());
		gsd.getPlacementWorker().setPolicies(null);
		
		if (targetVertex.requiresCreation()) {
			// if we didn't create it within this batch, we probably need to query for it.
			obtainVertexTargets(taskContext);
			if (targetVertex.requiresCreation()) {
				this.addVertex(database);
			}
		}

		if (domainEdgeId == 0) {
			this.createConnection(database);
		}
		time = (System.nanoTime() - time);
		logger.info(String.format("F,%d,%d", time, ConnectTask.ProcessCounter));
 }
    
    @Override
    public String getAuditLogEntry() 
    {      
    	return "ConnectTask";
    }
}   
