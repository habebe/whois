package com.objectivity.ig.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinitegraph.GraphDatabase;
import com.infinitegraph.pipelining.QueryTask;
import com.infinitegraph.pipelining.TargetEdge;
import com.infinitegraph.pipelining.TargetManager;
import com.infinitegraph.pipelining.TargetVertex;
import com.infinitegraph.pipelining.TaskContext;

public class WhoisTask extends QueryTask
{
  private static final Logger logger = LoggerFactory.getLogger(WhoisTask.class);

  private transient TargetVertex domainTargetVertex;
  private transient TargetVertex countryTargetVertex;
  private transient TargetVertex emailTargetVertex;
  private transient TargetVertex registrarTargetVertex;
  private transient TargetVertex[] nameServerTargetVertexes;

  private transient TargetEdge countryTargetEdge;
  private transient TargetEdge emailTargetEdge;
  private transient TargetEdge registrarTargetEdge;
  private transient TargetEdge[] nameServerTargetEdges;
  
  private transient String[] nameServers = null;

  private String country;
  private String registrar;
  private String email;
  private String nameServerStatement;

  public WhoisTask(String domain, String country, String registrar,
      String email, String nameServerStatement)
  {
    super(domain);
    this.set(country, registrar, email, nameServerStatement);
  }

  private void set(String country, String registrar, String email,
      String nameServerStatement)
  {
    this.markModified();
    this.country = country;
    this.registrar = registrar;
    this.email = email;
    this.nameServerStatement = nameServerStatement;
  }

  private String getCountry()
  {
    fetch();
    return this.country;
  }

  private String getRegistrar()
  {
    fetch();
    return this.registrar;
  }

  private String getEmail()
  {
    fetch();
    return this.email;
  }

  private String[] getNameServers()
  {
    fetch();
    if (this.nameServerStatement != null)
    {
      nameServers = this.nameServerStatement.split("\\|");
    }
    return nameServers;
  }

  @Override
  public void setPrimaryKeys(TargetManager targetManager)
  {
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Domain.class,
        "name");
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Country.class,
        "name");
    targetManager
        .setPrimaryKey(com.whois.WBNP.model.vertex.Email.class, "name");
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Registrar.class,
        "name");
    targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.NameServer.class,
        "name");
  }

  @Override
  public void obtainTargets(TargetManager targetManager)
  {
    this.domainTargetVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Domain.class, this.getQueryTerm());
    this.countryTargetVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Country.class, this.getCountry());
    this.emailTargetVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Email.class, this.getEmail());
    this.registrarTargetVertex = targetManager.getTargetVertex(
        com.whois.WBNP.model.vertex.Registrar.class, this.getRegistrar());

    this.countryTargetEdge = targetManager.getTargetEdge
        (this.domainTargetVertex, this.countryTargetVertex);      
    this.emailTargetEdge = targetManager.getTargetEdge
        (this.domainTargetVertex, this.emailTargetVertex);      
    this.registrarTargetEdge = targetManager.getTargetEdge
        (this.domainTargetVertex, this.registrarTargetVertex);      
    
    String[] nameServers = this.getNameServers();

    if (nameServers != null)
    {
      this.nameServerTargetVertexes = new TargetVertex[nameServers.length];
      this.nameServerTargetEdges = new TargetEdge[nameServers.length];
      for (int i = 0; i < nameServers.length; i++)
      {
        TargetVertex nameServerTargetVertex = targetManager.getTargetVertex(
            com.whois.WBNP.model.vertex.NameServer.class, nameServers[i]); 
        this.nameServerTargetVertexes[i] = nameServerTargetVertex;
        this.nameServerTargetEdges[i] = targetManager.getTargetEdge
            (this.domainTargetVertex, nameServerTargetVertex);
      }
    }
  }

  static long ProcessCounter = 0;

  private long addEdge(com.infinitegraph.GraphDatabase database,
      com.infinitegraph.BaseEdge baseEdge, long source, long target)
  {
    if (source < target)
      return database.addEdge(baseEdge, source, target,
          com.infinitegraph.EdgeKind.OUTGOING, (short) 0);
    else
      return database.addEdge(baseEdge, target, source,
          com.infinitegraph.EdgeKind.INCOMING, (short) 0);
  }

  @Override
  public void process(TaskContext taskContext)
  {
    WhoisTask.ProcessCounter += 1;
    long time = System.nanoTime();
    com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph
        .getSessionData(taskContext.getSession());
    gsd.getPlacementWorker().setPolicies(null);

    GraphDatabase database = taskContext.getGraph();

    if (this.domainTargetVertex.requiresCreation())
    {
      com.whois.WBNP.model.vertex.Domain domain = new com.whois.WBNP.model.vertex.Domain();
      domain.set_name(this.getQueryTerm());
      database.addVertex(domain);
      domainTargetVertex.setId(domain.getId());
      domain.updateIndexes();
    }

    // process country
    if (this.countryTargetVertex.requiresCreation())
    {
      CountryTask subTask = new CountryTask(this.getCountry(),
          this.domainTargetVertex.getId(taskContext.getSession()));
      database.submitPipelineTask(subTask);
      this.countryTargetVertex.setCreateTaskSubmitted();
    }
    else if (this.countryTargetEdge.requiresCreation())
    {
      long id = this.addEdge(database, new com.whois.WBNP.model.edge.OwnerCountry(),
          this.domainTargetVertex.getId(taskContext.getSession()),
          this.countryTargetVertex.getId(taskContext.getSession()));
      this.countryTargetEdge.setEdgeId(id);
    }
    // process email
    if (this.emailTargetVertex.requiresCreation())
    {
      EmailTask subTask = new EmailTask(this.getEmail(),
          this.domainTargetVertex.getId(taskContext.getSession()));
      database.submitPipelineTask(subTask);
      this.emailTargetVertex.setCreateTaskSubmitted();
    }
    else if (this.emailTargetEdge.requiresCreation())
    {
      long id = this.addEdge(database, new com.whois.WBNP.model.edge.OwnerEmail(),
          this.domainTargetVertex.getId(taskContext.getSession()),
          this.emailTargetVertex.getId(taskContext.getSession()));
      this.emailTargetEdge.setEdgeId(id);
    }
    // process registrar
    if (this.registrarTargetVertex.requiresCreation())
    {
      RegistarTask subTask = new RegistarTask(this.getRegistrar(),
          this.domainTargetVertex.getId(taskContext.getSession()));
      database.submitPipelineTask(subTask);
      this.registrarTargetVertex.setCreateTaskSubmitted();
    }
    else if (this.registrarTargetEdge.requiresCreation())
    {
      long id = this.addEdge(database, new com.whois.WBNP.model.edge.OwnerRegistrar(),
          this.domainTargetVertex.getId(taskContext.getSession()),
          this.registrarTargetVertex.getId(taskContext.getSession()));
      this.registrarTargetEdge.setEdgeId(id);
    }
    // name servers
    for (int i = 0; i < nameServerTargetVertexes.length; i++)
    {
      if (this.nameServerTargetVertexes[i].requiresCreation())
      {
        // TODO - cleanup the Name server code, perhaps with an inner
        // class to hold the string and the target vertex.
        NameServerTask subTask = new NameServerTask(nameServers[i],
            this.domainTargetVertex.getId(taskContext.getSession()));
        database.submitPipelineTask(subTask);
        this.nameServerTargetVertexes[i].setCreateTaskSubmitted();
      }
      else if (this.nameServerTargetEdges[i].requiresCreation())
      {
        long id = this.addEdge(database, new com.whois.WBNP.model.edge.NameServerEdge(),
            this.domainTargetVertex.getId(taskContext.getSession()),
            this.nameServerTargetVertexes[i].getId(taskContext.getSession()));
        this.nameServerTargetEdges[i].setEdgeId(id);
      }
    }
    time = (System.nanoTime() - time);
    logger.info(String.format("B,%d,%d", time, WhoisTask.ProcessCounter));
  }

  @Override
  public String getAuditLogEntry()
  {
    return "WhoisTask";
  }
}
