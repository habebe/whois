package com.objectivity.ig.utility;

import com.infinitegraph.GraphDatabase;
import com.infinitegraph.pipelining.TargetManager;
import com.infinitegraph.pipelining.TargetVertex;
import com.infinitegraph.pipelining.TaskContext;

public class EmailTask extends ConnectTask
{

  public EmailTask(String term, long domainId)
  {
    super(term, domainId);
  }

  @Override
  public void setPrimaryKeys(TargetManager targetManager)
  {
    targetManager.setPrimaryKey
      (com.whois.WBNP.model.vertex.Email.class, "name");
  }

  @Override
  public TargetVertex obtainTargetVertex(TargetManager targetManager)
  {
    return targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Email.class, this.getQueryTerm());
  }

  @Override
  protected long addVertex(GraphDatabase database)
  {
    com.whois.WBNP.model.vertex.Email email = new com.whois.WBNP.model.vertex.Email();
    email.set_name(this.getQueryTerm());
    database.addVertex(email);
    email.updateIndexes();
    return email.getId();
  }

  @Override
  protected long createConnection(TaskContext taskContext, long domainId, long targetId)
  {
    com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerEmail();
    long edgeId = taskContext.getGraph().addEdge(baseEdge, domainId, targetId,
        com.infinitegraph.EdgeKind.OUTGOING, (short) 0);
    return edgeId;
}

}
