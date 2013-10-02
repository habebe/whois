package com.objectivity.ig.utility;

import com.infinitegraph.GraphDatabase;
import com.infinitegraph.pipelining.TargetManager;
import com.infinitegraph.pipelining.TargetVertex;
import com.infinitegraph.pipelining.TaskContext;

public class RegistarTask extends ConnectTask
{

  public RegistarTask(String term, long domainId)
  {
    super(term, domainId);
  }

  @Override
  public void setPrimaryKeys(TargetManager targetManager)
  {
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Registrar.class,
        "name");
  }

  @Override
  public TargetVertex obtainTargetVertex(TargetManager targetManager)
  {
    return targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Registrar.class, this.getQueryTerm());
  }

  protected long addVertex(GraphDatabase database)
  {
    com.whois.WBNP.model.vertex.Registrar registrar = new com.whois.WBNP.model.vertex.Registrar();
    registrar.set_name(this.getQueryTerm());
    database.addVertex(registrar);
    registrar.updateIndexes();
    return registrar.getId();
  }

  protected long createConnection(TaskContext taskContext, long domainId, long targetId)
  {
    com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerRegistrar();
    long edgeId = taskContext.getGraph().addEdge(baseEdge, domainId, targetId,
        com.infinitegraph.EdgeKind.OUTGOING, (short) 0);
    return edgeId;
  }
}
