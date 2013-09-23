package com.objectivity.ig.utility;
import org.slf4j.*;

public abstract class ConnectTask extends com.infinitegraph.pipelining.QueryTask
{
    protected static final Logger logger = LoggerFactory.getLogger(ConnectTask.class);
    protected long domainId;
    protected transient com.infinitegraph.BaseVertex vertex = null;
    protected transient boolean connected = false;
    
    @SuppressWarnings("unchecked")
    protected java.util.HashMap<String,Long> getTargetEntryMap(com.infinitegraph.pipelining.TaskContext taskContext,String className) 
    {
	java.util.HashMap<String,Long> map = (java.util.HashMap<String,Long>)taskContext.getTaskGroupData(className);
	if(map == null) 
	    {
		map = new java.util.HashMap<String,Long>();
		taskContext.setTaskGroupData(className,map);
	    }
	return map;
    }	

    protected Long getDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext,String className,String targetKey)
    {
    	java.util.HashMap<String,Long> map = this.getTargetEntryMap(taskContext,className);
        return map.get(targetKey);
    }
    
    protected Long setDataForTarget(com.infinitegraph.pipelining.TaskContext taskContext, 
					     String className,String targetKey,Long entry)
    {
        java.util.HashMap<String,Long> map = this.getTargetEntryMap(taskContext,className);
        return map.put(targetKey, entry);
    }
	
    public ConnectTask(String term,long domainId)
    {
	super(term);
	this.set(domainId);
    }
    
    private void set(long domainId)
    {
	this.domainId = domainId;
	this.markModified();
    }

    protected long getDomainId()
    {
	fetch();
	return this.domainId;
    }
    
    protected com.infinitegraph.BaseVertex query(com.infinitegraph.pipelining.TaskContext taskContext,
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
			if(entry > 0)
			    vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry));
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

    private static QueryResultHandler RESULT_QUALIFIER = new QueryResultHandler();
    protected com.infinitegraph.BaseVertex query(com.infinitegraph.pipelining.TaskContext taskContext,
						 com.infinitegraph.GraphDatabase database,
						 String className,String queryTerm,
						 com.objy.db.internal.Query qualifier
						 )
    {
	com.infinitegraph.BaseVertex vertex = null;
	if(queryTerm != null)
	    {
		Long entry = this.getDataForTarget(taskContext,className,queryTerm);
		if(entry != null)
		    {
			if(entry > 0)
			    vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry));
		    }
		else
		    {
			RESULT_QUALIFIER.reset();
			qualifier.execute(RESULT_QUALIFIER);
			Object found = RESULT_QUALIFIER.found(taskContext.getSession().getFD());
			if(found != null)
			    {
				vertex = (com.infinitegraph.BaseVertex)found;
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
    
    
    protected com.infinitegraph.BaseVertex query(com.infinitegraph.pipelining.TaskContext taskContext,
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
			if(entry > 0)
			    vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry));
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

    static long PreProcessCounter = 0;
    static long ProcessCounter = 0;		
    @Override
    public void preProcess(com.infinitegraph.pipelining.TaskContext taskContext) 
    {
	ConnectTask.PreProcessCounter += 1;
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	long time = System.nanoTime();
	if(this.performQueryUsingQualifier(taskContext,database) > 0)
	    //if(this.performQuery(taskContext,database) > 0)
	    this.checkConnectivity();
	time = (System.nanoTime() - time);
	logger.info(String.format("D,%d,%d",time,ConnectTask.PreProcessCounter));
    }

    static public com.objy.query.ObjectQualifier CountryObjectQualifier = null;
    static public com.objy.query.ObjectQualifier EmailObjectQualifier = null;
    static public com.objy.query.ObjectQualifier RegistrarObjectQualifier = null;
    static public com.objy.query.ObjectQualifier NameServerObjectQualifier = null;
    static public com.objy.query.ObjectQualifier IpObjectQualifier = null;
    static public void initializeQualifiers()
    {
	if(CountryObjectQualifier == null)
	    {
		CountryObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Country.class.getName(),"(name == $A:string)");
		EmailObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Email.class.getName(),"(name == $A:string)");
		RegistrarObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Registrar.class.getName(),"(name == $A:string)");
		NameServerObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.NameServer.class.getName(),"(name == $A:string)");
		IpObjectQualifier = new com.objy.query.ObjectQualifier(com.whois.WBNP.model.vertex.Ip.class.getName(),"(ip == $A:string)");
	    }
    }

    static public com.objy.db.internal.Query CountryQuery = null;
    static public com.objy.db.internal.Query EmailQuery = null;
    static public com.objy.db.internal.Query RegistrarQuery = null;
    static public com.objy.db.internal.Query NameServerQuery = null;
    static public com.objy.db.internal.Query IpQuery = null;
    static public void initializeQuery()
    {
        if(CountryQuery == null)
            {
                CountryQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Country.class.getName(),"(name == $A:string)");
                EmailQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Email.class.getName(),"(name == $A:string)");
		RegistrarQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Registrar.class.getName(),"(name == $A:string)");
                NameServerQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.NameServer.class.getName(),"(name == $A:string)");
                IpQuery = new com.objy.db.internal.Query(com.whois.WBNP.model.vertex.Ip.class.getName(),"(ip == $A:string)");
            }
    }



    protected abstract com.infinitegraph.BaseVertex addVertex(com.infinitegraph.pipelining.TaskContext taskContext,
							      com.infinitegraph.GraphDatabase database);
    protected abstract void createConnection(com.infinitegraph.GraphDatabase database);
    protected abstract int  performQuery(com.infinitegraph.pipelining.TaskContext taskContext,
					 com.infinitegraph.GraphDatabase database);
    protected abstract int  performQueryUsingQualifier(com.infinitegraph.pipelining.TaskContext taskContext,
						       com.infinitegraph.GraphDatabase database);
    protected abstract int  performQueryUsingResultHandler(com.infinitegraph.pipelining.TaskContext taskContext,
							   com.infinitegraph.GraphDatabase database);
    protected abstract String getClassName();
    
    protected void checkConnectivity()
    {
	if(this.vertex != null)
	    {
		
		for(com.infinitegraph.EdgeHandle edgeHandle : this.vertex.getEdges())
		    {
			com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
			if(domainId == vertexHandle.getId())
			    {
				this.connected = true;
				return;
			    }
		    }
	    }
    }

    @Override
    public void process(com.infinitegraph.pipelining.TaskContext taskContext)
    {
	ConnectTask.ProcessCounter += 1;
	logger.info(String.format("C,2,%d,%d",System.currentTimeMillis(),ConnectTask.ProcessCounter));
	com.infinitegraph.GraphDatabase database = taskContext.getGraph();
	com.infinitegraph.impl.GraphSessionData gsd = com.infinitegraph.impl.InfiniteGraph.getSessionData(taskContext.getSession());
	gsd.getPlacementWorker().setPolicies(null);
	if(this.vertex == null)
	    {
		Long entry = this.getDataForTarget(taskContext,
						   getClassName(),
						   getQueryTerm());
		if((entry != null) && (entry > 0))
		    {
			this.vertex = (com.infinitegraph.BaseVertex)(database.getVertex(entry));
			this.checkConnectivity();
		    }
		if(this.vertex == null)
		    this.vertex = this.addVertex(taskContext,database);
	    }
	if(this.connected == false)
	    {
		this.createConnection(database);
	    }
	logger.info(String.format("C,3,%d,%d",System.currentTimeMillis(),ConnectTask.ProcessCounter));
    }
    
    @Override
    public String getAuditLogEntry() 
    {      
	return "ConnectTask";
    }
}   
