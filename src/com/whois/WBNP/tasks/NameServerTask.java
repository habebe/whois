package com.objectivity.ig.utility;

import com.infinitegraph.GraphDatabase;
import com.infinitegraph.pipelining.TargetManager;
import com.infinitegraph.pipelining.TargetVertex;
import com.infinitegraph.pipelining.TaskContext;


public class NameServerTask extends ConnectTask
{

  public NameServerTask(String term, long domainId)
  {
    super(term, domainId);
  }

  @Override
  public void setPrimaryKeys(TargetManager targetManager)
  {
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.NameServer.class,
        "name");
  }

  @Override
  public TargetVertex obtainTargetVertex(TargetManager targetManager)
  {
    return targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.NameServer.class, this.getQueryTerm());
  }

  @Override
  protected long addVertex(GraphDatabase database)
  {
    com.whois.WBNP.model.vertex.NameServer nameServer = new com.whois.WBNP.model.vertex.NameServer();
    nameServer.set_name(this.getQueryTerm());
    database.addVertex(nameServer);
    nameServer.updateIndexes();
    return nameServer.getId();
  }

  @Override
  protected long createConnection(TaskContext taskContext, long domainId, long targetId)
  {
    com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.NameServerEdge();
    long edgeId = taskContext.getGraph().addEdge(baseEdge, domainId, targetId,
        com.infinitegraph.EdgeKind.OUTGOING, (short) 0);
    return edgeId;
  }

}
