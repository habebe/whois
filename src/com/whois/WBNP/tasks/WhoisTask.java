package com.objectivity.ig.utility;
import java.util.HashMap;

import org.slf4j.*;

import com.infinitegraph.EdgeHandle;
import com.infinitegraph.impl.ObjectivityUtilities;

//class Node
//{
//    public com.infinitegraph.BaseVertex vertex = null;
//    public String data = null;
//    public boolean connected = false;
//}


public class WhoisTask extends com.infinitegraph.pipelining.QueryTask
{
    private static final Logger logger = LoggerFactory.getLogger(WhoisTask.class);
    //private transient Node[] nameServerNodes = null;

    private transient com.infinitegraph.pipelining.TargetVertex domainTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex	countryTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex	emailTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex registrarTargetVertex;
    private transient HashMap<Long, EdgeHandle> neighborMap = new HashMap<Long, EdgeHandle>();
    private transient com.infinitegraph.EdgeHandle countryEdgeHandle;
    private transient com.infinitegraph.EdgeHandle emailEdgeHandle;
    private transient com.infinitegraph.EdgeHandle registrarEdgeHandle;
    
    private String country;
    private String registrar;
    private String email;
    private String nameServerStatement;

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

    
    @Override
    public void setPrimaryKeys(com.infinitegraph.pipelining.TargetManager targetManager)
    {
        targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Domain.class,
                "name");
        targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Country.class,
                "name");
        targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Email.class,
                "name");
        targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.Registrar.class,
                "name");
        targetManager.setPrimaryKey(com.whois.WBNP.model.vertex.NameServer.class,
                "name");
    }

    @Override
    public void obtainVertexTargets(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
		com.infinitegraph.pipelining.TargetManager targetManager = 
		      taskContext.getTargetManager();
		this.domainTargetVertex = targetManager.getTargetVertex(
		      com.whois.WBNP.model.vertex.Domain.class, this.getQueryTerm());
		this.countryTargetVertex = targetManager.getTargetVertex(
		      com.whois.WBNP.model.vertex.Country.class, this.getCountry());
		neighborMap.put(countryTargetVertex.getId(), null);
		this.emailTargetVertex = targetManager.getTargetVertex(
		          com.whois.WBNP.model.vertex.Email.class, this.getEmail());
		neighborMap.put(emailTargetVertex.getId(), null);
		this.registrarTargetVertex = targetManager.getTargetVertex(
		          com.whois.WBNP.model.vertex.Registrar.class, this.getRegistrar());
		neighborMap.put(registrarTargetVertex.getId(), null);

		// TODO
//		if(nameServers != null)
//		{
//			this.nameServerNodes = new Node[nameServers.length];
//			for(int i=0;i<nameServers.length;i++)
//			{
//				this.nameServerNodes[i] = targetManager.getTargetVertex(
//						com.whois.WBNP.model.vertex.NameServer.class,
//								     nameServers[i]);
//				this.nameServerNodes[i].data = nameServers[i];
//			}
//		}
    }

    @Override
    public void checkConnectivity(
        com.infinitegraph.pipelining.TaskContext taskContext)
    {
		boolean status = false;
		if(domainTargetVertex.wasFound())
	    {
			long time = System.nanoTime();
			com.whois.WBNP.model.vertex.Domain domainVertex = (com.whois.WBNP.model.vertex.Domain) ObjectivityUtilities
			          .getObjectFromLong(taskContext.getSession(),
			              domainTargetVertex.getId());
			domainVertex.getEdgeToNeighbors(neighborMap);
			if (countryTargetVertex.wasFound())
				countryEdgeHandle = neighborMap.get(countryTargetVertex.getId());
			if (emailTargetVertex.wasFound())
				emailEdgeHandle = neighborMap.get(emailTargetVertex.getId());
			if (registrarTargetVertex.wasFound())
				registrarEdgeHandle = neighborMap.get(registrarTargetVertex.getId());

			// TODO
			// we need to add similar code for the NameServers...
			
			time = (System.nanoTime()-time);
			int size = domainVertex.getHandle().getEdgeCount();
                logger.info(String.format("C,%d,%d,%d,Domain=%s",time,WhoisTask.ProcessCounter,size,this.getQueryTerm()));
	    }
    }

    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		

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
		com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
		gsd.getPlacementWorker().setPolicies(null);

		boolean vertexCreated = false;
		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		
		if(this.domainTargetVertex.requiresCreation())
		{
			// if we didn't create it within this batch, we probably need to query for it.
			obtainVertexTargets(taskContext);
			if (domainTargetVertex.requiresCreation()) {
				com.whois.WBNP.model.vertex.Domain domain = new com.whois.WBNP.model.vertex.Domain();
				domain.set_name(this.getQueryTerm());
				database.addVertex(domain);
				domainTargetVertex.setId(domain.getId());
				domain.updateIndexes();
		    }
		}
		
		if ((countryEdgeHandle == null) || (emailEdgeHandle == null) || (registrarEdgeHandle == null))
		{
			//this.checkConnectivityMapped();
			this.checkConnectivity(taskContext);
		}
		// process country....
		if(countryTargetVertex.requiresCreation())
		{
			CountryTask subTask = new CountryTask(this.getCountry(),this.domainTargetVertex.getId());
			database.submitPipelineTask(subTask);

	    }
		else if(countryEdgeHandle == null)
		{
			this.addEdge(database,
				     new com.whois.WBNP.model.edge.OwnerCountry(),
				     this.domainTargetVertex.getId(),
				     this.countryTargetVertex.getId());
	    }
		// process email
		if(this.emailTargetVertex.requiresCreation())
		{
			EmailTask subTask = new EmailTask(this.getEmail(),this.domainTargetVertex.getId());
			database.submitPipelineTask(subTask);
		}
		else if(emailEdgeHandle == null)
		{
			this.addEdge(database,
				     new com.whois.WBNP.model.edge.OwnerEmail(),
				     this.domainTargetVertex.getId(),
				     this.emailTargetVertex.getId()
				     );
		}

		// process registrar...
		if(registrarTargetVertex.requiresCreation())
	    {
			RegistarTask subTask = new RegistarTask(this.getRegistrar(),this.domainTargetVertex.getId());
			database.submitPipelineTask(subTask);
	    }
		else if(registrarEdgeHandle == null)
		{
			this.addEdge(database,
				     new com.whois.WBNP.model.edge.OwnerRegistrar(),
				     this.domainTargetVertex.getId(),
				     this.registrarTargetVertex.getId());
		}
		// TODO
//		if(this.nameServerNodes != null)
//		    {
//			for(Node nameServer:this.nameServerNodes)
//			    {
//				if(nameServer.vertex == null)
//				    {
//					if(nameServer.data != null)
//					    {
//						nameServer.data = nameServer.data.trim();
//						if(nameServer.data.length() > 0)
//						    {
//							NameServerTask subTask = new NameServerTask(nameServer.data,this.domainNode.vertex.getId());
//							database.submitPipelineTask(subTask);
//						    }
//					    }
//				    }
//				else if(isConnected(nameServer) == false)
//				    {
//					this.addEdge(database,
//						     new com.whois.WBNP.model.edge.NameServerEdge(),
//						     this.domainNode.vertex.getId(),
//						     nameServer.vertex.getId());
//				    }
//			    }
//		    }
//		
//	    }
	time  = (System.nanoTime() - time);
	logger.info(String.format("B,%d,%d",time,WhoisTask.ProcessCounter));
    }
	

    @Override
    public String getAuditLogEntry() 
    {      
	return "WhoisTask";
    }
}   
