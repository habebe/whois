package com.objectivity.ig.utility;
import org.slf4j.*;

public class WebTask extends com.infinitegraph.pipelining.QueryTask
{
    private static final Logger logger = LoggerFactory.getLogger(WebTask.class);
    private String Ip;
    private double Volume;

    private transient com.whois.WBNP.model.vertex.Domain domainVertex = null;
    private transient com.whois.WBNP.model.vertex.Ip     ipVertex     = null;
    private transient com.whois.WBNP.model.edge.IpDomain ipDomainEdge = null;
    
    @SuppressWarnings("unchecked")
    private java.util.HashMap<String,Long> getTargetEntryMap(com.infinitegraph.pipelining.TaskContext taskContext,String className) 
    {
	java.util.HashMap<String,Long> map = (java.util.HashMap<String,Long>)taskContext.getTaskGroupData(className);
	if(map == null) 
	    {
		map = new java.util.HashMap<String,Long>();
		taskContext.setTaskGroupData(className,map);
	    }
	return map;
    }	

    private Long getDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext,String className,String targetKey)
    {
    	java.util.HashMap<String,Long> map = this.getTargetEntryMap(taskContext,className);
        return map.get(targetKey);
    }
        
    private Long setDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext, String className,String targetKey,Long entry)
    {
        java.util.HashMap<String,Long> map = this.getTargetEntryMap(taskContext,className);
        return map.put(targetKey, entry);
    }
	
    public WebTask(String Domain,String Ip,double Volume)
    {
	super(Domain);
	this.set(Ip,Volume);
    }
    
    private void set(String Ip,double Volume)
    {
	this.Ip     = Ip;
	this.Volume = Volume;
	this.markModified();
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
 
    private com.infinitegraph.BaseVertex query(com.infinitegraph.pipelining.TaskContext taskContext,
					       com.infinitegraph.GraphDatabase database,
					       String className,String queryTerm
					       )
    {
	com.infinitegraph.BaseVertex vertex = null;
	if(queryTerm != null)
	    {
		Long entry = this.getDataForTarget(taskContext,className,queryTerm);
		if(entry != null)
		    {
			if(entry.longValue() > 0)
			    vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.longValue()));
		    }
		else
		    {
			try
			    {
				com.infinitegraph.Query<com.infinitegraph.BaseVertex> query = database.createQuery(className,queryTerm);
				vertex = query.getSingleResult();
				if(vertex != null)
				    this.setDataForTarget(taskContext,className,
							  queryTerm,
							  new Long(vertex.getId()));
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
	    }
	return vertex;
    }
    static com.objy.query.ObjectQualifier DomainObjectQualifier = null;
    static com.objy.query.ObjectQualifier IpObjectQualifier = null;
    static void initializeQualifiers()
    {
        if(DomainObjectQualifier == null)
            {
                DomainObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Domain.class.getName(),"(name == $A:string)");
                IpObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Ip.class.getName(),"(ip == $A:string)");
	    }
    }

    private com.infinitegraph.BaseVertex query(com.infinitegraph.pipelining.TaskContext taskContext,
                       com.infinitegraph.GraphDatabase database,
                       String className,String queryTerm,
                       com.objy.query.ObjectQualifier qualifier
                       )
    {
        com.infinitegraph.BaseVertex vertex = null;
        if(queryTerm != null)
            {
                Long entry = this.getDataForTarget(taskContext,className,queryTerm);
                if(entry != null)
                    {
                        if(entry.longValue() > 0)
                            vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry.longValue()));
                    }
                else
                    {
                        qualifier.setStringVarValue("A",queryTerm);
                        com.objy.db.app.Iterator iterator = taskContext.getSession().getFD().scan(className,qualifier);
                        if(iterator.hasNext())
                            {
                                vertex = (com.infinitegraph.BaseVertex)iterator.next();
                            }
                        if(vertex != null)
                            this.setDataForTarget(taskContext,className,
                                                  queryTerm,
                                                  new Long(vertex.getId()));
                        else
                            this.setDataForTarget(taskContext,className,
                                                  queryTerm,
                                                  new Long(-1));
                    }
            }
        return vertex;
    }


    private void performQuery(com.infinitegraph.pipelining.TaskContext taskContext,
			      com.infinitegraph.GraphDatabase database)
    {
	
	this.domainVertex = (com.whois.WBNP.model.vertex.Domain)this.query(taskContext,database,
									   com.whois.WBNP.model.vertex.Domain.class.getName(),
									   String.format("(name == \"%s\")",this.getQueryTerm()));
	this.ipVertex     = (com.whois.WBNP.model.vertex.Ip)this.query(taskContext,database,
								       com.whois.WBNP.model.vertex.Ip.class.getName(),
								       String.format("(ip == \"%s\")",this.getIp()));
    }

    private void performQueryUsingQualifier(com.infinitegraph.pipelining.TaskContext taskContext,
					    com.infinitegraph.GraphDatabase database)
    {
        this.initializeQualifiers();
        this.domainVertex    = (com.whois.WBNP.model.vertex.Domain)this.query(taskContext,database,
									      com.whois.WBNP.model.vertex.Domain.class.getName(),
									      this.getQueryTerm(),
									      DomainObjectQualifier
									      );
        this.ipVertex   = (com.whois.WBNP.model.vertex.Ip)this.query(taskContext,database,
								     com.whois.WBNP.model.vertex.Ip.class.getName(),
								     this.getIp(),
								     IpObjectQualifier
								     );
    }
    static long IpDomainTypeId   = -1;
    private static void initializeEdgeTypes(com.infinitegraph.GraphDatabase database)
    {
	if(WebTask.IpDomainTypeId == -1)
	    WebTask.IpDomainTypeId = database.getTypeId(com.whois.WBNP.model.edge.IpDomain.class.getName());
    }

    
    private void checkConnectivity(com.infinitegraph.GraphDatabase database)
    {
	if((domainVertex != null) && (ipVertex != null))
	    {
		long time = System.nanoTime();
		long id = ipVertex.getId();
		com.infinitegraph.EdgeHandle handle = this.domainVertex.getEdgeToNeighbor(id);
                if(handle != null)
                    ipDomainEdge = (com.whois.WBNP.model.edge.IpDomain)handle.getEdge();
		time = (System.nanoTime()-time);
		int size = this.domainVertex.getHandle().getEdgeCount();
                logger.info(String.format("C,%d,%d,%d",time,WebTask.ProcessCounter,size));
	    }
    }

    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		
    @Override
    public void preProcess(com.infinitegraph.pipelining.TaskContext taskContext) 
    {
	WebTask.PreProcessCounter += 1;
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	long time = System.nanoTime();
	this.performQuery(taskContext,database);
	//this.performQueryUsingQualifier(taskContext,database);
	this.checkConnectivity(database);
	time = (System.nanoTime() - time);
	logger.info(String.format("A,%d,%d",time,WebTask.PreProcessCounter));
    }
    
    @Override
    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
	WebTask.ProcessCounter += 1;
	long time = System.nanoTime();
	com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
	gsd.getPlacementWorker().setPolicies(null);
	
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	if(this.domainVertex == null)
	    {
		domainVertex = new com.whois.WBNP.model.vertex.Domain();
		domainVertex.set_name(this.getQueryTerm());
		database.addVertex(domainVertex);
		this.setDataForTarget(taskContext,
				      com.whois.WBNP.model.vertex.Domain.class.getName(),
				      this.getQueryTerm(),
				      new Long(domainVertex.getId()));
		domainVertex.updateIndexes();
	    }
	
	if(this.ipVertex != null)
	    {
		if(ipDomainEdge == null)
		    {
			ipDomainEdge = new com.whois.WBNP.model.edge.IpDomain();
			database.addEdge(ipDomainEdge,
					 this.domainVertex,
					 this.ipVertex,
					 com.infinitegraph.EdgeKind.OUTGOING,
					 (short)0);
			ipDomainEdge.set_volume(this.getVolume());
		    }
		else
		    ipDomainEdge.set_volume(ipDomainEdge.get_volume() + this.getVolume());
	    }
	else
	    {
		//SUBMIT TASK
		IpTask subTask = new IpTask(this.getIp(),this.domainVertex.getId(),this.getVolume());
		database.submitPipelineTask(subTask);
	    }
	time  = (System.nanoTime() - time);
	logger.info(String.format("B,%d,%d",time,WebTask.ProcessCounter));
    }
	

    @Override
    public String getAuditLogEntry() 
    {      
	return "WebTask";
    }
}   
