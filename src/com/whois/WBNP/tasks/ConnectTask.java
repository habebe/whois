package com.objectivity.ig.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinitegraph.pipelining.TargetManager;
import com.infinitegraph.pipelining.TargetVertex;
import com.infinitegraph.pipelining.TargetEdge;
import com.infinitegraph.pipelining.TaskContext;
import com.infinitegraph.GraphDatabase;

public abstract class ConnectTask extends
    com.infinitegraph.pipelining.QueryTask
{
  protected static final Logger logger = LoggerFactory
      .getLogger(ConnectTask.class);
  private long domainId = 0;
  private transient TargetEdge domainEdge = null;
  private transient TargetVertex targetVertex = null;

  public ConnectTask(String term, long domainId)
  {
    super(term);
    this.set(domainId);
  }

  private void set(long domainId)
  {
    this.markModified();
    this.domainId = domainId;
  }

  protected long getDomainId()
  {
    fetch();
    return this.domainId;
  }

  static long ProcessCounter = 0;

  protected abstract long addVertex(GraphDatabase database);

  protected abstract long createConnection(TaskContext taskContext, long domainId, long targetId);

  protected abstract TargetVertex obtainTargetVertex(TargetManager targetManager);
  
  @Override
  public void obtainTargets(TargetManager targetManager)
  {
    targetVertex = obtainTargetVertex(targetManager);
    domainEdge = targetManager.getTargetEdge(domainId, targetVertex);
  }

  @Override
  public void process(TaskContext taskContext)
  {
    ConnectTask.ProcessCounter += 1;
    long time = System.nanoTime();
    GraphDatabase database = taskContext.getGraph();
    com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
        .getSessionData(taskContext.getSession());
    gsd.getPlacementWorker().setPolicies(null);

    if (targetVertex.requiresCreation())
    {
      targetVertex.setId(this.addVertex(database));
    }

    if (domainEdge.requiresCreation())
    {
      domainEdge.setEdgeId(this.createConnection(taskContext,
          domainId, targetVertex.getId(taskContext.getSession())));
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
