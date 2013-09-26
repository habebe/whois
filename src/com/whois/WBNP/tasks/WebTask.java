package com.objectivity.ig.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinitegraph.VertexHandle;
import com.infinitegraph.impl.ObjectivityUtilities;
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

  private transient long ipDomainEdge = 0;

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
  public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
  {
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Domain.class,
        "name");
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Ip.class, "ip");
  }

  @Override
  public void obtainVertexTargets(
      com.infinitegraph.pipelining.TaskContext taskContext)
  {
    com.infinitegraph.pipelining.TargetManager targetManager = 
        taskContext.getTargetManager();
    this.domainVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Domain.class, this.getQueryTerm());
    this.ipVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Ip.class, this.getIp());
  }

  @Override
  public void checkConnectivity(
      com.infinitegraph.pipelining.TaskContext taskContext)
  {
    if (this.domainVertex.wasFound() && this.ipVertex.wasFound())
    {
    	Session session = taskContext.getSession();
    	//logger.info(String.format(" >> C, %d, %d", domainVertex.getId(session), ipVertex.getId(session)));
      // there can only be an existing edge if both vertices were found
      long time = System.nanoTime();
      VertexHandle domainHandle = taskContext.getGraph().getVertexHandle(
              this.domainVertex.getId(session));
      // TODO - we need to have getEdgeToNeighbor() to be on the Vertex handle.
      com.whois.WBNP.model.vertex.Domain asDomain = 
    		  (com.whois.WBNP.model.vertex.Domain) domainHandle.getVertex();
      com.infinitegraph.EdgeHandle handle = asDomain.getEdgeToNeighbor(ipVertex
          .getId(session));
      if (handle != null)
        ipDomainEdge = handle.getEdge().getId();
      time = (System.nanoTime() - time);
      int size = domainHandle.getEdgeCount();
      logger.info(String.format("C,%d,%d,%d", time, WebTask.ProcessCounter,
          size));
    }
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
    
    //logger.info(" >> B ");
    if (this.domainVertex.requiresCreation())
    {
      //logger.info(" >> B1 ");
      com.whois.WBNP.model.vertex.Domain newDomainVertex = new com.whois.WBNP.model.vertex.Domain();
      newDomainVertex.set_name(this.getQueryTerm());
      database.addVertex(newDomainVertex);
      this.domainVertex.setId(newDomainVertex.getId());
      newDomainVertex.updateIndexes();
    }

    //logger.info(" >> B2 ");
    if (this.ipVertex.requiresCreation())
    {
      // SUBMIT TASK
      IpTask subTask = new IpTask(this.getIp(), this.domainVertex.getId(session),
          this.getVolume());
      database.submitPipelineTask(subTask);
    }
    else
    {
      if (ipDomainEdge == 0)
      {
          //logger.info(String.format(" >> B2_1,%d,%d", domainVertex.getId(session), ipVertex.getId(session)));
      	
        com.whois.WBNP.model.edge.IpDomain newIpDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
        database.addEdge(newIpDomainEdge, this.domainVertex.getId(session),
            this.ipVertex.getId(session), com.infinitegraph.EdgeKind.OUTGOING,
            (short) 0);
        newIpDomainEdge.set_volume(this.getVolume());
        //logger.info(" << B2_1 ");
      }
      else
      {
          //logger.info(" >> B2_2 ");     	
        com.whois.WBNP.model.edge.IpDomain existingIpDomainEdge = (com.whois.WBNP.model.edge.IpDomain) 
        		taskContext.getGraph().getEdge(ipDomainEdge);
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