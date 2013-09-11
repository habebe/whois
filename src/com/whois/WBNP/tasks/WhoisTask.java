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

    private String Country;
    private String Registrar;
    private String Email;
    private String NameServerStatement;

    class VertexIDEntry
    {
	public long id;
	public VertexIDEntry(long id)
	{
	    this.id = id;	
	}
    }	

    @SuppressWarnings("unchecked")
    private java.util.HashMap<String,VertexIDEntry> getTargetEntryMap(com.infinitegraph.pipelining.TaskContext taskContext,String className) 
    {
	java.util.HashMap<String,VertexIDEntry> map = (java.util.HashMap<String,VertexIDEntry>)taskContext.getTaskGroupData(className);
	if(map == null) 
	    {
		map = new java.util.HashMap<String,VertexIDEntry>();
		taskContext.setTaskGroupData(className,map);
	    }
	return map;
    }	

    private VertexIDEntry getDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext,String className,String targetKey)
    {
    	java.util.HashMap<String,VertexIDEntry> map = this.getTargetEntryMap(taskContext,className);
        return map.get(targetKey);
    }
        
    private VertexIDEntry setDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext, String className,String targetKey,VertexIDEntry entry)
    {
        java.util.HashMap<String,VertexIDEntry> map = this.getTargetEntryMap(taskContext,className);
        return map.put(targetKey, entry);
    }
	
    public WhoisTask(String Domain,String Country,String Registrar,String Email,String NameServerStatement)
    {
	super(Domain);
	this.set(Country,Registrar,Email,NameServerStatement);
    }
    
    private void set(String Country,String Registrar,String Email,String NameServerStatement)
    {
	this.Country  = Country;
	this.Registrar = Registrar;
	this.Email    = Email;
	this.NameServerStatement = NameServerStatement;
	this.markModified();
    }

    private String getCountry()
    {
	fetch();
	return this.Country;
    }

    private String getRegistrar()
    {
	fetch();
	return this.Registrar;
    }
 
    private String getEmail()
    {
	fetch();
	return this.Email;
    }

    private String[] getNameServers()
    {
	fetch();
	String[] nameServers = null;
	if(this.NameServerStatement != null)
	    {
		nameServers = this.NameServerStatement.split("\\|");
	    }
	return nameServers;
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
		VertexIDEntry entry = this.getDataForTarget(taskContext,className,queryTerm);
		node = new Node();
		if(entry != null)
		    {
			if(entry.id > 0)
			    node.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.id));
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
							  new VertexIDEntry(node.vertex.getId()));
				else
				    this.setDataForTarget(taskContext,className,
							  queryTerm,
							  new VertexIDEntry(-1));
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

    
    private void checkConnectivity()
    {
	boolean status = false;
	if(this.domainNode.vertex != null)
	    {
		
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
		
	    }
    }

    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		
    @Override
    public void preProcess(com.infinitegraph.pipelining.TaskContext taskContext) 
    {
	WhoisTask.PreProcessCounter += 1;
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	logger.info(String.format("D,0,%d,%d",System.currentTimeMillis(),WhoisTask.PreProcessCounter));
	this.performQuery(taskContext,database);
	if(this.numberOfNodesFound > 0)
	    {
		this.initializeEdgeTypes(database);
		this.checkConnectivity();
	    }
	logger.info(String.format("D,1,%d,%d",System.currentTimeMillis(),WhoisTask.PreProcessCounter));
    }
    
    @Override
    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
	WhoisTask.ProcessCounter += 1;
	logger.info(String.format("D,2,%d,%d",System.currentTimeMillis(),WhoisTask.ProcessCounter));
	if(this.domainNode != null)
	    {
		com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
		gsd.getPlacementWorker().setPolicies(null);
		
		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		if(this.domainNode.vertex == null)
		    {
			com.whois.WBNP.model.vertex.Domain domain = new com.whois.WBNP.model.vertex.Domain();
			domain.set_name(this.getQueryTerm());
			this.domainNode.vertex = domain;
			database.addVertex(this.domainNode.vertex);
			this.setDataForTarget(taskContext,
					      com.whois.WBNP.model.vertex.Domain.class.getName(),
					      this.getQueryTerm(),
					      new VertexIDEntry(domain.getId()));
			domain.updateIndexes();
		    }
		if(this.countryNode != null)
		    {
			if(this.countryNode.vertex == null)
			    {
				CountryTask subTask = new CountryTask(this.getCountry(),this.domainNode.vertex.getId());
				database.submitPipelineTask(subTask);

			    }
			else if(this.countryNode.connected == false)
			    {
				com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerCountry();
				database.addEdge(baseEdge,this.domainNode.vertex,
						 this.countryNode.vertex,
						 com.infinitegraph.EdgeKind.OUTGOING,
						 (short)0);
				
			    }
		    }
		if(this.emailNode != null)
		    {
			if(this.emailNode.vertex == null)
			    {
				EmailTask subTask = new EmailTask(this.getEmail(),this.domainNode.vertex.getId());
				database.submitPipelineTask(subTask);
			    }
			else if(this.emailNode.connected == false)
			    {
				com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerEmail();
				database.addEdge(baseEdge,this.domainNode.vertex,
						 this.emailNode.vertex,
						 com.infinitegraph.EdgeKind.OUTGOING,
						 (short)0);
				
			    }
		    }
		if(this.registrarNode != null)
		    {
			if(this.registrarNode.vertex == null)
			    {
				RegistarTask subTask = new RegistarTask(this.getRegistrar(),this.domainNode.vertex.getId());
				database.submitPipelineTask(subTask);
			    }
			else if(this.registrarNode.connected == false)
			    {
				com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.OwnerRegistrar();
				database.addEdge(baseEdge,this.domainNode.vertex,
						 this.registrarNode.vertex,
						 com.infinitegraph.EdgeKind.OUTGOING,
						 (short)0);
				
			    }
		    }
		if(this.nameServerNodes != null)
		    {
			for(Node nameServer:this.nameServerNodes)
			    {
				if(nameServer.vertex == null)
				    {
					NameServerTask subTask = new NameServerTask(this.getCountry(),this.domainNode.vertex.getId());
					database.submitPipelineTask(subTask);
				    }
				else if(nameServer.connected == false)
				    {
					com.infinitegraph.BaseEdge baseEdge = new com.whois.WBNP.model.edge.NameServerEdge();
					database.addEdge(baseEdge,this.domainNode.vertex,
							 nameServer.vertex,
							 com.infinitegraph.EdgeKind.OUTGOING,
							 (short)0);
					
				    }
			    }
		    }
		
	    }
	logger.info(String.format("D,3,%d,%d",System.currentTimeMillis(),WhoisTask.ProcessCounter));
    }
	

    @Override
    public String getAuditLogEntry() 
    {      
	return "WhoisTask";
    }
}   