package com.objectivity.ig.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinitegraph.VertexHandle;
import com.objy.db.app.Session;

public class WebTask extends com.infinitegraph.pipelining.QueryTask
{

  private static final Logger logger = LoggerFactory.getLogger(WebTask.class);
  static long PreProcessCounter = 0;
  static long ProcessCounter = 0;

  private String Ip;
  private double Volume;

  private transient com.infinitegraph.pipelining.TargetVertex domainVertex = null;
  private transient com.infinitegraph.pipelining.TargetVertex ipVertex = null;

  private transient com.infinitegraph.pipelining.TargetEdge ipDomainEdge = null;

  public WebTask(String Domain, String Ip, double Volume)
  {
    super(Domain);
    this.set(Ip, Volume);
  }

  private void set(String Ip, double Volume)
  {
    this.markModified();
    this.Ip = Ip;
    this.Volume = Volume;
  }

  private String getIp()
  {
    fetch();
    return this.Ip;
  }

  private double getVolume()
  {
    fetch();
    return this.Volume;
  }

  @Override
  public void setPrimaryKeys(
      com.infinitegraph.pipelining.TargetManager targetManager)
  {
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Domain.class,
        "name");
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Ip.class, "ip");
  }

  @Override
  public void obtainTargets(com.infinitegraph.pipelining.TargetManager targetManager)
  {
    this.domainVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Domain.class, this.getQueryTerm());
    this.ipVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Ip.class, this.getIp());
    this.ipDomainEdge = targetManager.getTargetEdge(this.domainVertex, this.ipVertex);
  }

  @Override
  public void process(com.infinitegraph.pipelining.TaskContext taskContext)
  {
    WebTask.ProcessCounter += 1;
    long time = System.nanoTime();
    com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
        .getSessionData(taskContext.getSession());
    gsd.getPlacementWorker().setPolicies(null);

    com.infinitegraph.GraphDatabase database = taskContext.getGraph();
    Session session = taskContext.getSession();

    if (this.domainVertex.requiresCreation())
    {
      com.whois.WBNP.model.vertex.Domain newDomainVertex = new com.whois.WBNP.model.vertex.Domain();
      newDomainVertex.set_name(this.getQueryTerm());
      database.addVertex(newDomainVertex);
      this.domainVertex.setId(newDomainVertex.getId());
      newDomainVertex.updateIndexes();
    }

    if (this.ipVertex.requiresCreation())
    {
      // SUBMIT TASK
      IpTask subTask = new IpTask(this.getIp(),
          this.domainVertex.getId(session), this.getVolume());
      database.submitPipelineTask(subTask);
    }
    else
    {
      if (this.ipDomainEdge.requiresCreation())
      {
        com.whois.WBNP.model.edge.IpDomain newIpDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
        database.addEdge(newIpDomainEdge, this.domainVertex.getId(session),
            this.ipVertex.getId(session), com.infinitegraph.EdgeKind.OUTGOING,
            (short) 0);
        newIpDomainEdge.set_volume(this.getVolume());
        this.ipDomainEdge.setEdgeId(newIpDomainEdge.getId());
      }
      else
      {
    	  logger.info(String.format(">>Edge doesn't reguire creation... ID(%d)", ipDomainEdge.getEdgeId()));
        com.whois.WBNP.model.edge.IpDomain existingIpDomainEdge = (com.whois.WBNP.model.edge.IpDomain) taskContext
            .getGraph().getEdge(ipDomainEdge.getEdgeId());
        existingIpDomainEdge.set_volume(existingIpDomainEdge.get_volume()
            + this.getVolume());
      }
    }
    time = (System.nanoTime() - time);
    logger.info(String.format("B,%d,%d", time, WebTask.ProcessCounter));
  }

  @Override
  public String getAuditLogEntry()
  {
    return "WebTask";
  }
}