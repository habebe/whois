package com.objectivity.ig.utility;
import java.util.HashMap;

import org.slf4j.*;

import com.infinitegraph.EdgeHandle;
import com.infinitegraph.VertexHandle;
import com.infinitegraph.impl.ObjectivityUtilities;

public class WhoisTask extends com.infinitegraph.pipelining.QueryTask
{
    private static final Logger logger = LoggerFactory.getLogger(WhoisTask.class);

    private transient com.infinitegraph.pipelining.TargetVertex domainTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex	countryTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex	emailTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex registrarTargetVertex;
    private transient com.infinitegraph.pipelining.TargetVertex[] nameServerTargetVertexes;
    
    private transient String[] nameServers = null;
    
    private transient HashMap<Long, EdgeHandle> neighborMap = new HashMap<Long, EdgeHandle>();
        
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
		this.emailTargetVertex = targetManager.getTargetVertex(
		          com.whois.WBNP.model.vertex.Email.class, this.getEmail());
		this.registrarTargetVertex = targetManager.getTargetVertex(
		          com.whois.WBNP.model.vertex.Registrar.class, this.getRegistrar());

		 String[] nameServers = this.getNameServers();

		if(nameServers != null)
		{
			this.nameServerTargetVertexes = new com.infinitegraph.pipelining.TargetVertex[nameServers.length];
			for(int i=0;i<nameServers.length;i++)
			{
				this.nameServerTargetVertexes[i] = targetManager.getTargetVertex(
						com.whois.WBNP.model.vertex.NameServer.class, nameServers[i]);
			}
		}
    }

    @Override
    public void checkConnectivity(
        com.infinitegraph.pipelining.TaskContext taskContext)
    { 	
		boolean status = false;
		if(domainTargetVertex.wasFound())
	    {
			long time = System.nanoTime();
			VertexHandle domainVertexHandle = taskContext.getGraph().getVertexHandle(
			              domainTargetVertex.getId(taskContext.getSession()));
			// fill the neighbor map
			if (countryTargetVertex.wasFound())
				neighborMap.put(countryTargetVertex.getId(taskContext.getSession()), null);
			if (emailTargetVertex.wasFound())
				neighborMap.put(emailTargetVertex.getId(taskContext.getSession()), null);
			if (registrarTargetVertex.wasFound())
				neighborMap.put(registrarTargetVertex.getId(taskContext.getSession()), null);
			for(int i=0;i<nameServerTargetVertexes.length;i++)
			{
				if (this.nameServerTargetVertexes[i].wasFound())
					neighborMap.put(this.nameServerTargetVertexes[i] .getId(taskContext.getSession()), null);
			}
			
			domainVertexHandle.getEdgeToNeighbors(neighborMap);
			
			time = (System.nanoTime()-time);
			int size = domainVertexHandle.getEdgeCount();
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

		com.infinitegraph.GraphDatabase database = taskContext.getGraph();
		
		boolean justCreatedDomain = false;
		
		if(this.domainTargetVertex.requiresCreation())
		{
			com.whois.WBNP.model.vertex.Domain domain = new com.whois.WBNP.model.vertex.Domain();
			domain.set_name(this.getQueryTerm());
			database.addVertex(domain);
			domainTargetVertex.setId(domain.getId());
			domain.updateIndexes();
			justCreatedDomain = true;
		}
		else 
		{
			for (EdgeHandle eh : this.neighborMap.values())
			{
				if (eh == null)
				{
					this.checkConnectivity(taskContext);
					break;
				}
			}
		}
		
		// process country....
		if(countryTargetVertex.requiresCreation())
		{
			CountryTask subTask = new CountryTask(this.getCountry(),
					this.domainTargetVertex.getId(taskContext.getSession()));
			database.submitPipelineTask(subTask);

	    }
		else if(justCreatedDomain || 
				this.neighborMap.get(this.countryTargetVertex.getId(taskContext.getSession())) == null)
		{
			this.addEdge(database,
				     new com.whois.WBNP.model.edge.OwnerCountry(),
				     this.domainTargetVertex.getId(taskContext.getSession()),
				     this.countryTargetVertex.getId(taskContext.getSession()));
	    }
		// process email
		if(this.emailTargetVertex.requiresCreation())
		{
			EmailTask subTask = new EmailTask(this.getEmail(),
					this.domainTargetVertex.getId(taskContext.getSession()));
			database.submitPipelineTask(subTask);
		}
		else if(justCreatedDomain ||
				this.neighborMap.get(this.emailTargetVertex.getId(taskContext.getSession())) == null)
		{
			this.addEdge(database,
				     new com.whois.WBNP.model.edge.OwnerEmail(),
				     this.domainTargetVertex.getId(taskContext.getSession()),
				     this.emailTargetVertex.getId(taskContext.getSession())
				     );
		}

		// process registrar...
		if(registrarTargetVertex.requiresCreation())
	    {
			RegistarTask subTask = new RegistarTask(this.getRegistrar(),
					this.domainTargetVertex.getId(taskContext.getSession()));
			database.submitPipelineTask(subTask);
	    }
		else if(justCreatedDomain ||
				this.neighborMap.get(this.registrarTargetVertex.getId(taskContext.getSession())) == null)
		{
			this.addEdge(database,
				     new com.whois.WBNP.model.edge.OwnerRegistrar(),
				     this.domainTargetVertex.getId(taskContext.getSession()),
				     this.registrarTargetVertex.getId(taskContext.getSession()));
		}
		// name servers
		for(int i=0;i<nameServerTargetVertexes.length;i++)
		{
			if (this.nameServerTargetVertexes[i].requiresCreation())
			{
				// TODO - cleanup the Name server code, perhaps with an inner 
				//        class to hold the string and the target vertex.
				NameServerTask subTask = new NameServerTask(nameServers[i],
						this.domainTargetVertex.getId(taskContext.getSession()));
				database.submitPipelineTask(subTask);
		    }
			else if(justCreatedDomain ||
					this.neighborMap.get(this.countryTargetVertex.getId(taskContext.getSession())) == null)
		    {
				this.addEdge(database,
				     new com.whois.WBNP.model.edge.NameServerEdge(),
				     this.domainTargetVertex.getId(taskContext.getSession()),
				     this.nameServerTargetVertexes[i] .getId(taskContext.getSession()));
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
