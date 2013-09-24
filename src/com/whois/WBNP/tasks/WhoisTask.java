package com.objectivity.ig.utility;
import org.slf4j.*;

class Node
{
    public com.infinitegraph.BaseVertex vertex = null;
    public String data = null;
    public boolean connected = false;
}


public class WhoisTask extends com.infinitegraph.pipelining.QueryTask
{
    private static final Logger logger = LoggerFactory.getLogger(WhoisTask.class);
    private transient Node domainNode  = null;
    private transient Node countryNode = null;
    private transient Node emailNode   = null;
    private transient Node registrarNode = null;
    private transient Node[] nameServerNodes = null;

    private String country;
    private String registrar;
    private String email;
    private String nameServerStatement;

    @SuppressWarnings("unchecked")
    private java.util.HashMap<String,Long> getTargetEntryMap(com.infinitegraph.pipelining.TaskContext taskContext,
								      String className) 
    {
	java.util.HashMap<String,Long> map = (java.util.HashMap<String,Long>)taskContext.getTaskGroupData(className);
	if(map == null) 
	    {
		map = new java.util.HashMap<String,Long>();
		taskContext.setTaskGroupData(className,map);
	    }
	return map;
    }	

    private Long getDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext,
				  String className,
				  String targetKey)
    {
    	java.util.HashMap<String,Long> map = this.getTargetEntryMap(taskContext,className);
        return map.get(targetKey);
    }
        
    private Long setDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext, 
				  String className,
				  String targetKey,
				  Long entry)
    {
        java.util.HashMap<String,Long> map = this.getTargetEntryMap(taskContext,className);
        return map.put(targetKey, entry);
    }
	
    public WhoisTask(String domain,String country,String registrar,String email,String nameServerStatement)
    {
	super(domain);
	this.set(country,registrar,email,nameServerStatement);
    }
    
    private void set(String country,String registrar,String email,String nameServerStatement)
    {
	this.country  = country;
	this.registrar = registrar;
	this.email    = email;
	this.nameServerStatement = nameServerStatement;
	this.markModified();
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
	String[] nameServers = null;
	if(this.nameServerStatement != null)
	    {
		nameServers = this.nameServerStatement.split("\\|");
	    }
	return nameServers;
    }

    static com.objy.query.ObjectQualifier DomainObjectQualifier = null;
    static com.objy.query.ObjectQualifier CountryObjectQualifier = null;
    static com.objy.query.ObjectQualifier EmailObjectQualifier = null;
    static com.objy.query.ObjectQualifier RegistrarObjectQualifier = null;
    static com.objy.query.ObjectQualifier NameServerObjectQualifier = null;
    static void initializeQualifiers()
    {
	if(DomainObjectQualifier == null)
	    {
		DomainObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Domain.class.getName(),
									   "(name == $A:string)");	
		CountryObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Country.class.getName(),
									    "(name == $A:string)");
		EmailObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Email.class.getName(),
									  "(name == $A:string)");
		RegistrarObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Registrar.class.getName(),
									      "(name == $A:string)");
		NameServerObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.NameServer.class.getName(),
									       "(name == $A:string)");
	    }
    }

    static com.objy.db.internal.Query DomainQuery = null;
    static com.objy.db.internal.Query CountryQuery = null;
    static com.objy.db.internal.Query EmailQuery = null;
    static com.objy.db.internal.Query RegistrarQuery = null;
    static com.objy.db.internal.Query NameServerQuery = null;
    static void initializeQuery()
    {
	if(DomainQuery == null)
	    {
		DomainQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Domain.class.getName(),
									   "(name == $A:string)");	
		CountryQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Country.class.getName(),
									    "(name == $A:string)");
		EmailQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Email.class.getName(),
									  "(name == $A:string)");
		RegistrarQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Registrar.class.getName(),
									      "(name == $A:string)");
		NameServerQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.NameServer.class.getName(),
									       "(name == $A:string)");
	    }
    }

    private static QueryResultHandler ResultHandler = null;
    private QueryResultHandler getResultHandler()
    {
	if(ResultHandler == null)
	    {
		ResultHandler = new QueryResultHandler();
	    }
	return ResultHandler;
    }

    private Node query(com.infinitegraph.pipelining.TaskContext taskContext,
		       com.infinitegraph.GraphDatabase database,
		       String className,String queryTerm,
		       com.objy.db.internal.Query qualifier
		       )
    {
	Node node = null;
	if(queryTerm != null)
	    {
		Long entry = this.getDataForTarget(taskContext,className,queryTerm);
		node = new Node();
		if(entry != null)
		    {
			if(entry.longValue() > 0)
			    node.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.longValue()));
		    }
		else
		    {
			qualifier.setStringVarValue("A",queryTerm);
			this.getResultHandler().reset();
			qualifier.execute(ResultHandler);
			Object found = ResultHandler.found(taskContext.getSession().getFD());
			if(found != null)
			    {
				node.vertex = (com.infinitegraph.BaseVertex)found;
			    }
			if(node.vertex != null)
			    this.setDataForTarget(taskContext,className,
						  queryTerm,
						  new Long(node.vertex.getId()));
			else
			    this.setDataForTarget(taskContext,className,
						  queryTerm,
						  new Long(-1));
		    }
		if(node.vertex != null)
		    this.numberOfNodesFound += 1;
	    }
	return node;
    }
    

    private Node query(com.infinitegraph.pipelining.TaskContext taskContext,
		       com.infinitegraph.GraphDatabase database,
		       String className,String queryTerm,
		       com.objy.query.ObjectQualifier qualifier
		       )
    {
	Node node = null;
	if(queryTerm != null)
	    {
		Long entry = this.getDataForTarget(taskContext,className,queryTerm);
		node = new Node();
		if(entry != null)
		    {
			if(entry.longValue() > 0)
			    node.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.longValue()));
		    }
		else
		    {
			qualifier.setStringVarValue("A",queryTerm);
			com.objy.db.app.Iterator iterator = taskContext.getSession().getFD().scan(className,qualifier);
			if(iterator.hasNext())
			    {
				node.vertex = (com.infinitegraph.BaseVertex)iterator.next();
			    }
			if(node.vertex != null)
			    this.setDataForTarget(taskContext,className,
						  queryTerm,
						  new Long(node.vertex.getId()));
			else
			    this.setDataForTarget(taskContext,className,
						  queryTerm,
						  new Long(-1));
		    }
		if(node.vertex != null)
		    this.numberOfNodesFound += 1;
	    }
	return node;
    }
    
    private transient int numberOfNodesFound = 0;
    private Node query(com.infinitegraph.pipelining.TaskContext taskContext,
		       com.infinitegraph.GraphDatabase database,
		       String className,String queryTerm
		       )
    {
	Node node = null;
	if(queryTerm != null)
	    {
		Long entry = this.getDataForTarget(taskContext,className,queryTerm);
		node = new Node();
		if(entry != null)
		    {
			if(entry.longValue() > 0)
			    node.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.longValue()));
		    }
		else
		    {
			try
			    {
				com.infinitegraph.Query<com.infinitegraph.BaseVertex> query = database.createQuery(className,queryTerm);
				node.vertex = query.getSingleResult();
				if(node.vertex != null)
				    this.setDataForTarget(taskContext,className,
							  queryTerm,
							  new Long(node.vertex.getId()));
				else
				    this.setDataForTarget(taskContext,className,
							  queryTerm,
							  new Long(-1));
			    }
			catch(com.infinitegraph.GraphException e)
			    {
				logger.error("QUERY FAILED - " + queryTerm + " - " + e.toString());
				e.printStackTrace();
			    }
		    }
		if(node.vertex != null)
		    this.numberOfNodesFound += 1;
	    }
	return node;
    }

    private void performQueryUsingQualifier(com.infinitegraph.pipelining.TaskContext taskContext,
					    com.infinitegraph.GraphDatabase database)
    {
	this.initializeQualifiers();
	this.domainNode    = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Domain.class.getName(),
					this.getQueryTerm(),
					DomainObjectQualifier
					);
	this.numberOfNodesFound = 0;
	this.countryNode   = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Country.class.getName(),
					this.getCountry(),
					CountryObjectQualifier
					);
	this.emailNode     = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Email.class.getName(),
					this.getEmail(),
					EmailObjectQualifier
					);
	this.registrarNode = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Registrar.class.getName(),
					this.getRegistrar(),
					RegistrarObjectQualifier
					);
	String[] nameServers = this.getNameServers();
	if(nameServers != null)
	    {
		this.nameServerNodes = new Node[nameServers.length];
		for(int i=0;i<nameServers.length;i++)
		    {
			this.nameServerNodes[i] = this.query(taskContext,database,
							     com.whois.WBNP.model.vertex.NameServer.class.getName(),
							     nameServers[i],
							     NameServerObjectQualifier
							     );
			this.nameServerNodes[i].data = nameServers[i];
		    }
	    }
    }


    private void performQueryUsingResultHandler(com.infinitegraph.pipelining.TaskContext taskContext,
						com.infinitegraph.GraphDatabase database)
    {
	this.initializeQuery();
	this.domainNode    = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Domain.class.getName(),
					this.getQueryTerm(),
					DomainQuery
					);
	this.numberOfNodesFound = 0;
	this.countryNode   = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Country.class.getName(),
					this.getCountry(),
					CountryQuery
					);
	this.emailNode     = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Email.class.getName(),
					this.getEmail(),
					EmailQuery
					);
	this.registrarNode = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Registrar.class.getName(),
					this.getRegistrar(),
					RegistrarQuery
					);
	String[] nameServers = this.getNameServers();
	if(nameServers != null)
	    {
		this.nameServerNodes = new Node[nameServers.length];
		for(int i=0;i<nameServers.length;i++)
		    {
			this.nameServerNodes[i] = this.query(taskContext,database,
							     com.whois.WBNP.model.vertex.NameServer.class.getName(),
							     nameServers[i],
							     NameServerQuery
							     );
			this.nameServerNodes[i].data = nameServers[i];
		    }
	    }
    }



    private void performQuery(com.infinitegraph.pipelining.TaskContext taskContext,
			      com.infinitegraph.GraphDatabase database)
    {
	
	this.domainNode    = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Domain.class.getName(),
					String.format("(name == \"%s\")",this.getQueryTerm()));
	this.numberOfNodesFound = 0;
	this.countryNode   = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Country.class.getName(),
					String.format("(name == \"%s\")",this.getCountry()));
	this.emailNode     = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Email.class.getName(),
					String.format("(name == \"%s\")",this.getEmail()));
	this.registrarNode = this.query(taskContext,database,
					com.whois.WBNP.model.vertex.Registrar.class.getName(),
					String.format("(name == \"%s\")",this.getRegistrar()));
	String[] nameServers = this.getNameServers();
	if(nameServers != null)
	    {
		this.nameServerNodes = new Node[nameServers.length];
		for(int i=0;i<nameServers.length;i++)
		    {
			this.nameServerNodes[i] = this.query(taskContext,database,
							     com.whois.WBNP.model.vertex.Registrar.class.getName(),
							     String.format("(name == \"%s\")",nameServers[i]));
			this.nameServerNodes[i].data = nameServers[i];
		    }
	    }
    }

    static long OwnerCountryTypeId   = -1;
    static long OwnerEmailTypeId     = -1;
    static long OwnerRegistrarTypeId = -1;
    static long NameServerTypeId     = -1;
    private static void initializeEdgeTypes(com.infinitegraph.GraphDatabase database)
    {
	if(WhoisTask.OwnerCountryTypeId == -1)
	    WhoisTask.OwnerCountryTypeId = database.getTypeId(com.whois.WBNP.model.edge.OwnerCountry.class.getName());
	if(WhoisTask.OwnerEmailTypeId == -1)
	    WhoisTask.OwnerEmailTypeId = database.getTypeId(com.whois.WBNP.model.edge.OwnerEmail.class.getName());
	if(WhoisTask.OwnerRegistrarTypeId == -1)
	    WhoisTask.OwnerRegistrarTypeId = database.getTypeId(com.whois.WBNP.model.edge.OwnerRegistrar.class.getName());
	if(WhoisTask.NameServerTypeId == -1)
	    WhoisTask.NameServerTypeId = database.getTypeId(com.whois.WBNP.model.edge.NameServerEdge.class.getName());
    }

    private transient java.util.HashMap<Long,com.infinitegraph.EdgeHandle> connectivityMap = null;
    private void checkConnectivityMapped()
    {
	
	if(this.domainNode.vertex != null)
	    {
		connectivityMap = new java.util.HashMap<Long,com.infinitegraph.EdgeHandle>();
		if((countryNode != null) && (countryNode.vertex != null))
		    connectivityMap.put(countryNode.vertex.getId(),null);
		if((emailNode != null) && (emailNode.vertex != null))
		    connectivityMap.put(emailNode.vertex.getId(),null);
		if((this.registrarNode != null) && (this.registrarNode.vertex != null))
		    connectivityMap.put(this.registrarNode.vertex.getId(),null);
		for(Node node:this.nameServerNodes)
		    {
			if(node.vertex != null)
			    connectivityMap.put(node.vertex.getId(),null);
		    }
		if(connectivityMap.size() > 0)
		    {
			//this.domainNode.vertex.getEdgeToNeighbors(connectivityMap);
		    }
	    }
    }

    private boolean isConnected(Node node)
    {
	boolean status = node.connected;
	/*
	if(connectivityMap != null)
	    status = (connectivityMap.get(node.vertex.getId()) != null); 
	*/
	return status;
    }

    private void checkConnectivity()
    {
	boolean status = false;
	if(this.domainNode.vertex != null)
	    {
		long time = System.nanoTime();
		
		for(com.infinitegraph.EdgeHandle edgeHandle : this.domainNode.vertex.getEdges())
		    {
			long edgeType = edgeHandle.getTypeId();
			if(edgeType == WhoisTask.OwnerCountryTypeId)
			    {
				if(this.countryNode != null)
				    {
					if(this.countryNode.vertex != null)
					    {
						com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
						if(this.countryNode.vertex.getId() == vertexHandle.getId())
						    {
							this.countryNode.connected = true;
						    }
					    }
				    }
			    }
			else if(edgeType == WhoisTask.OwnerEmailTypeId)
			    {
				if(this.emailNode != null)
				    {
					if(this.emailNode.vertex != null)
					    {
						com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
						if(this.emailNode.vertex.getId() == vertexHandle.getId())
						    {
							this.emailNode.connected = true;
						    }
					    }	
				    }
			    }
			else if(edgeType == WhoisTask.OwnerRegistrarTypeId)
			    {
				if(this.registrarNode != null)
				    {
					if(this.registrarNode.vertex != null)
					    {
						com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
						if(this.registrarNode.vertex.getId() == vertexHandle.getId())
						    {
							this.registrarNode.connected = true;
						    }
					    } 
				    }
			    }
			else if(edgeType == WhoisTask.NameServerTypeId)
			    {
				if(this.nameServerNodes != null)
				    {
					for(Node node:this.nameServerNodes)
					    {
						if(node.vertex != null)
						    {
							com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
							if(node.vertex.getId() == vertexHandle.getId())
							    {
								node.connected = true;
							    }
						    }
					    }	
				    }
			    }
		    }
		time = (System.nanoTime()-time);
		int size = this.domainNode.vertex.getHandle().getEdgeCount();
                logger.info(String.format("C,%d,%d,%d,Domain=%s",time,WhoisTask.ProcessCounter,size,this.getQueryTerm()));
	    }
    }

    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		
    @Override
    public void preProcess(com.infinitegraph.pipelining.TaskContext taskContext) 
    {
	WhoisTask.PreProcessCounter += 1;
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	long time = System.nanoTime();
	//	this.performQueryUsingQualifier(taskContext,database);
	this.performQueryUsingResultHandler(taskContext,database);
	time = (System.nanoTime() - time);
	logger.info(String.format("A,%d,%d",time,WhoisTask.PreProcessCounter));
    }
    
    private void addEdge(com.infinitegraph.GraphDatabase database,
			 com.infinitegraph.BaseEdge baseEdge,
			 long source,long target)
    {
	if(source < target)
	    database.addEdge(baseEdge,source,target,
			     com.infinitegraph.EdgeKind.OUTGOING,
			     (short)0);
	else
	    database.addEdge(baseEdge,target,source,
			     com.infinitegraph.EdgeKind.INCOMING,
			     (short)0);
    }

    @Override
    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
	WhoisTask.ProcessCounter += 1;
	long time = System.nanoTime();
	if(this.domainNode != null)
	    {
		com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
		gsd.getPlacementWorker().setPolicies(null);
		boolean vertexCreated = false;
		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		if(this.domainNode.vertex == null)
		    {
			Long entry = this.getDataForTarget(taskContext,
								    com.whois.WBNP.model.vertex.Domain.class.getName(),
								    getQueryTerm());
			if((entry != null) && (entry.longValue() > 0))
			    {
				this.domainNode.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.longValue()));
			    }
		    }
		if(this.domainNode.vertex == null)
		    {
			vertexCreated = true;
			com.whois.WBNP.model.vertex.Domain domain = new com.whois.WBNP.model.vertex.Domain();
			domain.set_name(this.getQueryTerm());
			this.domainNode.vertex = domain;
			database.addVertex(this.domainNode.vertex);
			this.setDataForTarget(taskContext,
					      com.whois.WBNP.model.vertex.Domain.class.getName(),
					      this.getQueryTerm(),
					      new Long(domain.getId()));
			domain.updateIndexes();
		    }
		if(vertexCreated == false)
		    {
			initializeEdgeTypes(database);
			//this.checkConnectivityMapped();
			this.checkConnectivity();
		    }
		if(this.countryNode != null)
		    {
			if(this.countryNode.vertex == null)
			    {
				CountryTask subTask = new CountryTask(this.getCountry(),this.domainNode.vertex.getId());
				database.submitPipelineTask(subTask);

			    }
			else if(isConnected(this.countryNode) == false)
			    {
				this.addEdge(database,
					     new com.whois.WBNP.model.edge.OwnerCountry(),
					     this.domainNode.vertex.getId(),
					     this.countryNode.vertex.getId());
			    }
		    }
		if(this.emailNode != null)
		    {
			if(this.emailNode.vertex == null)
			    {
				EmailTask subTask = new EmailTask(this.getEmail(),this.domainNode.vertex.getId());
				database.submitPipelineTask(subTask);
			    }
			else if(isConnected(this.emailNode) == false)
			    {
				this.addEdge(database,
					     new com.whois.WBNP.model.edge.OwnerEmail(),
					     this.domainNode.vertex.getId(),
					     this.emailNode.vertex.getId()
					     );
			    }
		    }
		if(this.registrarNode != null)
		    {
			if(this.registrarNode.vertex == null)
			    {
				RegistarTask subTask = new RegistarTask(this.getRegistrar(),this.domainNode.vertex.getId());
				database.submitPipelineTask(subTask);
			    }
			else if(isConnected(registrarNode) == false)
			    {
				this.addEdge(database,
					     new com.whois.WBNP.model.edge.OwnerRegistrar(),
					     this.domainNode.vertex.getId(),
					     this.registrarNode.vertex.getId());
			    }
		    }
		if(this.nameServerNodes != null)
		    {
			for(Node nameServer:this.nameServerNodes)
			    {
				if(nameServer.vertex == null)
				    {
					if(nameServer.data != null)
					    {
						nameServer.data = nameServer.data.trim();
						if(nameServer.data.length() > 0)
						    {
							NameServerTask subTask = new NameServerTask(nameServer.data,this.domainNode.vertex.getId());
							database.submitPipelineTask(subTask);
						    }
					    }
				    }
				else if(isConnected(nameServer) == false)
				    {
					this.addEdge(database,
						     new com.whois.WBNP.model.edge.NameServerEdge(),
						     this.domainNode.vertex.getId(),
						     nameServer.vertex.getId());
				    }
			    }
		    }
		
	    }
	time  = (System.nanoTime() - time);
	logger.info(String.format("B,%d,%d",time,WhoisTask.ProcessCounter));
    }
	

    @Override
    public String getAuditLogEntry() 
    {      
	return "WhoisTask";
    }
}   
