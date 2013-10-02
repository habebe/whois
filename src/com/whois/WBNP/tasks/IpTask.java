package com.objectivity.ig.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinitegraph.EdgeHandle;
import com.infinitegraph.VertexHandle;
import com.whois.WBNP.model.edge.IpDomain;

public class IpTask extends com.infinitegraph.pipelining.QueryTask
{
  private double volume;
  private long domainId = 0;

  private transient EdgeHandle ipDomainEdgeHandle = null;
  private transient com.infinitegraph.pipelining.TargetVertex ipTargetVertex;

  private static final Logger logger = LoggerFactory.getLogger(IpTask.class);

  static long PreProcessCounter = 0;
  static long ProcessCounter = 0;

  public IpTask(String term, long domainId, double volume)
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
  public void setPrimaryKeys(
      com.infinitegraph.pipelining.TargetManager targetManager)
  {
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Ip.class, "ip");
  }

  @Override
  public void obtainTargets(com.infinitegraph.pipelining.TargetManager targetManager)
  {
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

  protected void createConnection(
      com.infinitegraph.pipelining.TaskContext taskContext)
  {
    com.whois.WBNP.model.edge.IpDomain ipDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
    ipDomainEdge.set_volume(this.getVolume());
    long ipId = this.ipTargetVertex.getId(taskContext.getSession());
    // logger.info(String.format(">> CC,%d,%d", ipId, domainId));
    if (domainId < ipId)
      taskContext.getGraph().addEdge(ipDomainEdge, domainId, ipId,
          com.infinitegraph.EdgeKind.OUTGOING, (short) 0);
    else
      taskContext.getGraph().addEdge(ipDomainEdge, ipId, domainId,
          com.infinitegraph.EdgeKind.INCOMING, (short) 0);
    // logger.info(" << CC");
  }

  public void process(com.infinitegraph.pipelining.TaskContext taskContext)
  {
    // logger.info(">> E <<");
    ProcessCounter += 1;
    long time = System.nanoTime();
    com.infinitegraph.GraphDatabase database = taskContext.getGraph();
    com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
        .getSessionData(taskContext.getSession());
    gsd.getPlacementWorker().setPolicies(null);

    if (ipTargetVertex.requiresCreation())
    {
      this.addVertex(database);
    }
    // logger.info(">> F <<");
    long timeA = (System.nanoTime() - time);
    if (ipDomainEdgeHandle == null)
    {
      this.createConnection(taskContext);
    }
    else
    {
      IpDomain ipDomainEdge = (IpDomain) ipDomainEdgeHandle.getEdge();
      ipDomainEdge.set_volume(ipDomainEdge.get_volume() + this.getVolume());
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
