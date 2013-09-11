/********************************************************************************

********************************************************************************/



package com.whois.WBNP.operations;

import com.infinitegraph.*;
import java.io.*;
import org.apache.commons.cli.*;

class Histogram
{
    public java.util.HashMap<Long,Long> map = new java.util.HashMap<Long,Long>();
    public java.util.List<Long> keys = null;
    
    public void add(Long key)
    {
        Long oValue = map.get(key);
        if(oValue == null)
            oValue = new Long(1);
        else
            oValue += 1;
        map.put(key,oValue);
    }

    public java.util.List<Long> getKeys()
    {
        if(keys == null)
        {
            if(map.size() > 0)
            {
                keys = new java.util.ArrayList<Long>(map.size());
                for(java.util.Map.Entry<Long,Long> entry:this.map.entrySet())
                {
                    Long key = entry.getKey();
                    keys.add(key);
                }
                java.util.Collections.sort(keys);
            }
        }
        return keys;
    }
}

class DensityData
{
    Histogram outgoing = new Histogram();
    Histogram bidirectional = new Histogram();
    Histogram incoming = new Histogram();
    
    public void add(Long key,com.infinitegraph.EdgeKind kind)
    {
        
        if(kind == com.infinitegraph.EdgeKind.OUTGOING)
            outgoing.add(key);
        else if(kind == com.infinitegraph.EdgeKind.BIDIRECTIONAL)
            bidirectional.add(key);
        else if(kind == com.infinitegraph.EdgeKind.INCOMING)
            incoming.add(key);
    }

    public void write(com.infinitegraph.GraphDatabase graph,ElementData element,long type,
                      Histogram vertexType,Histogram edgeType) throws Exception
    {
        String edgeName = GraphStat.GetShortName(graph.getTypeName(type));
        String elementName = element.getName(graph);
        if(outgoing.map.size() > 0)
        {
            String fileName = String.format("outgoing.%s.%s.dist",edgeName,elementName);
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            java.util.List<Long> keys = this.outgoing.getKeys();
            
            for(Long key:keys)
            {
                Long counter = this.outgoing.map.get(key);
                writer.printf("%d,%d\n",key,counter);
            }
            writer.close();   
        }
        if(bidirectional.map.size() > 0)
        {
            String fileName = String.format("bidirectional.%s.%s.dist",edgeName,elementName);
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            java.util.List<Long> keys = this.bidirectional.getKeys();
            for(Long key:keys)
            {
                Long counter = this.bidirectional.map.get(key);
                writer.printf("%d,%d\n",key,counter);
            }
            writer.close();   
        }
        if(incoming.map.size() > 0)
        {
            String fileName = String.format("incoming.%s.%s.dist",edgeName,elementName);
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            java.util.List<Long> keys = this.incoming.getKeys();
            for(Long key:keys)
            {
                Long counter = this.incoming.map.get(key);
                writer.printf("%d,%d\n",key,counter);
            }
            writer.close();   
        }
    }
}

class ElementData
{
    public long a;
    public long b;
    public ElementData(long a,long b)
    {
        this.a = a;
        this.b = b;
    }

    public int hashCode()
    {
        String s = String.format("%s.%s",a,b);
        return s.hashCode();
    }

    public boolean equals(Object o)
    {
        if (o == null || o.getClass() != this.getClass())
            return false;
        ElementData object = (ElementData) o;
        return ((this.a == object.a) && (this.b == object.b));
    }

    public String getName(com.infinitegraph.GraphDatabase graph)
    {
        String A = GraphStat.GetShortName(graph.getTypeName(a));
        String B = GraphStat.GetShortName(graph.getTypeName(b));
        return String.format("%s.%s",GraphStat.GetShortName(A),GraphStat.GetShortName(B));
    }

}

class Relationship
{
    private long type;
    private java.util.HashMap<ElementData,DensityData> map = new java.util.HashMap<ElementData,DensityData>();

    public Relationship(long type)
    {
        this.type = type;
    }
    
    public void add(ElementData element,Long counter,com.infinitegraph.EdgeKind kind)
    {
        DensityData density = map.get(element);
        if(density == null)
        {
            density = new DensityData();
            map.put(element,density);
        }
        density.add(counter,kind);

    }

    public void write(com.infinitegraph.GraphDatabase graph,Histogram vertexType,Histogram edgeType) throws Exception
    {
        for(java.util.Map.Entry<ElementData,DensityData> entry:this.map.entrySet())
        {
            ElementData element = entry.getKey();
            DensityData density = entry.getValue();
            density.write(graph,element,type,vertexType,edgeType);
        }
    }
}

public class GraphStat
{

    final static public String DefaultPropertFileName = "properties/bootstrap.properties";
    final static public String DefaultGraphName       = "whois";
    private String propertyFileName = null;
    private String graphName = null;
    private GraphDatabase database = null;

    private Histogram vertexTypes = new Histogram();
    private Histogram edgeTypes  = new Histogram();
    
    private java.util.HashMap<Long,Relationship> relationships = new java.util.HashMap<Long,Relationship>();
    private long limit = -1;

    private GraphStat(String name,String propertyFileName,long limit)
    {
        this.graphName = name;
        this.propertyFileName = propertyFileName;
        this.limit = limit;
    }

    private GraphDatabase getDatabase()
    {
        if (database == null) {
            try
            {
                this.database = GraphFactory.open(this.graphName,this.propertyFileName);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
        return this.database;
    }

    
    private Relationship getRelationship(long type)
    {
        Relationship relationship = relationships.get(type);
        if(relationship == null)
        {
            relationship = new Relationship(type);
            relationships.put(type,relationship);
        }
        return relationship;
    }


    class VData
    {
        public long type;
        public java.util.HashMap<Long,Long> outgoing = new java.util.HashMap<Long,Long>();
        public java.util.HashMap<Long,Long> incoming = new java.util.HashMap<Long,Long>();
        public java.util.HashMap<Long,Long> bidirectional = new java.util.HashMap<Long,Long>();
        public VData(long type)
        {
            this.type = type;
        }
    }
	private static long outIndex = 0;
	private static long outCounter = 0;
    private void analyze(com.infinitegraph.Vertex vertex)
    {
        long type = vertex.getTypeId();
        vertexTypes.add(type);
        java.util.HashMap<Long,VData> data = new java.util.HashMap<Long,VData>();
        Iterable<com.infinitegraph.navigation.Hop> hops = vertex.getNeighborHops();
        for(com.infinitegraph.navigation.Hop hop:hops)
        {
            if(hop.hasEdge())
            {
                com.infinitegraph.EdgeHandle edgeHandle = hop.getEdgeHandle();
                com.infinitegraph.EdgeKind kind = edgeHandle.getKind();
                com.infinitegraph.VertexHandle vertexHandle = edgeHandle.getPeer();
                long vertexType = vertexHandle.getTypeId();
                long edgeType = edgeHandle.getTypeId();

                if(kind == com.infinitegraph.EdgeKind.OUTGOING)
                {
                    VData vData = data.get(edgeType);
                    if(vData == null)
                        vData = new VData(edgeType);
                    data.put(edgeType,vData);
                    Long counter = vData.outgoing.get(vertexType);
                    if(counter == null)
                        counter = new Long(1);
                    else
                        counter += 1;
                    edgeTypes.add(edgeType);
                    vData.outgoing.put(vertexType,counter);
                }
                else if(kind == com.infinitegraph.EdgeKind.BIDIRECTIONAL)
                {
                    VData vData = data.get(edgeType);
                    if(vData == null)
                        vData = new VData(edgeType);
                    data.put(edgeType,vData);
                    Long counter = vData.bidirectional.get(vertexType);
                    if(counter == null)
                        counter = new Long(1);
                    else
                        counter += 1;
                    edgeTypes.add(edgeType);
                    vData.bidirectional.put(vertexType,counter);
                }
                else if(kind == com.infinitegraph.EdgeKind.INCOMING)
                {
                    VData vData = data.get(edgeType);
                    if(vData == null)
                        vData = new VData(edgeType);
                    data.put(edgeType,vData);
                    Long counter = vData.incoming.get(vertexType);
                    if(counter == null)
                        counter = new Long(1);
                    else
                        counter += 1;
                    //edgeTypes.add(edgeType);
                    vData.incoming.put(vertexType,counter);
                }
            }
        }
        for(java.util.Map.Entry<Long,VData> vEntry:data.entrySet())
        {
            Long edgeType = vEntry.getKey();
            VData vData    = vEntry.getValue();
            Relationship relationship = this.getRelationship(edgeType);
            for(java.util.Map.Entry<Long,Long> eEntry:vData.outgoing.entrySet())
            {
                Long targetType = eEntry.getKey();
                Long counter = eEntry.getValue();
		outCounter += counter;
                outIndex += 1;
                ElementData eData = new ElementData(type,targetType);
                relationship.add(eData,counter,com.infinitegraph.EdgeKind.OUTGOING);
		System.out.printf("index:[%d] sum:[%d] current:[%d]\n",outIndex,outCounter,counter);
            }
            for(java.util.Map.Entry<Long,Long> eEntry:vData.bidirectional.entrySet())
            {
                Long targetType = eEntry.getKey();
                Long counter = eEntry.getValue();
                ElementData eData = new ElementData(type,targetType);
                relationship.add(eData,counter,com.infinitegraph.EdgeKind.BIDIRECTIONAL);
            }
            for(java.util.Map.Entry<Long,Long> eEntry:vData.incoming.entrySet())
            {
                Long targetType = eEntry.getKey();
                Long counter = eEntry.getValue();
                ElementData eData = new ElementData(type,targetType);
                relationship.add(eData,counter,com.infinitegraph.EdgeKind.INCOMING);
            }
        }
    }

    public void analyze(com.infinitegraph.GraphDatabase graph)
    {
        
        Iterable<com.infinitegraph.Vertex> vertices = graph.getVertices();
        java.util.Iterator<com.infinitegraph.Vertex> iterator = vertices.iterator();
        boolean done = (!iterator.hasNext());
        long counter = 0;
        while(!done)
        {
            com.infinitegraph.Vertex vertex = iterator.next();
            this.analyze(vertex);
            done = (!iterator.hasNext());
            if(this.limit > 0)
            {
                if(this.limit <= counter)
                    done = true;
            }
            counter += 1;
            if((counter % 1000) == 0)
            {
                System.out.printf(".");
            }
            if((counter % 100000) == 0)
            {
                System.out.printf(" [%d]\n",counter);
            }
        }
        System.out.printf("\nComplete\n");
    }

    static public String GetShortName(String longName)
    {
        int i = longName.lastIndexOf('.');
        if(i > 0)
        {
            return longName.substring(i+1);
        }
        return longName;
    }

    public void writeVertexType(com.infinitegraph.GraphDatabase graph) throws Exception
    {
        PrintWriter writer = new PrintWriter("vertex.type", "UTF-8");
        for(java.util.Map.Entry<Long,Long> entry:this.vertexTypes.map.entrySet())
        {
            Long type = entry.getKey();
            Long counter = entry.getValue();
            writer.printf("%d,%s,%d\n",type,GraphStat.GetShortName(graph.getTypeName(type)),counter);
        }
        writer.close();
    }

    public void writeEdgeType(com.infinitegraph.GraphDatabase graph) throws Exception
    {
        PrintWriter writer = new PrintWriter("edge.type", "UTF-8");
        for(java.util.Map.Entry<Long,Long> entry:this.edgeTypes.map.entrySet())
        {
            Long type = entry.getKey();
            Long counter = entry.getValue();
            writer.printf("%d,%s,%d\n",type,GraphStat.GetShortName(graph.getTypeName(type)),counter);
        }
        writer.close();
    }

    public void writeEdgeDistribution(com.infinitegraph.GraphDatabase graph) throws Exception
    {
        for(java.util.Map.Entry<Long,Relationship> entry:this.relationships.entrySet())
        {
            Relationship relationship = entry.getValue();
            relationship.write(graph,vertexTypes,edgeTypes);
        }        
    }

    static private void ShowHelp(Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.printHelp(GraphStat.class.getName(),options);
    }

    public static void main(String[] arguments) throws Exception
    {
        Options options = new Options();
        CommandLine commandLine = null;
        String propertyFileName = Bootstrap.DefaultPropertFileName;
        String graphName = Bootstrap.DefaultGraphName;
        final String helpMessage        = "Print this message.";
        final String graphNameMessage   = "Graph database name default=" + graphName;
        final String propertyMessage    = "File path of the property name default=" + propertyFileName;

        options.addOption(new Option("help",helpMessage));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(graphNameMessage).create("graph"));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription(propertyMessage).create("property"));
        options.addOption(OptionBuilder.withArgName("integer").hasArg().withDescription("The number of vertices to examine. default: -1 (all vertices).").create("limit"));


        CommandLineParser parser = new PosixParser();
        try
        {
                commandLine = parser.parse( options, arguments);
                if(commandLine.hasOption("help"))
                {
                        GraphStat.ShowHelp(options);
                }
                else
                {
                        propertyFileName = commandLine.getOptionValue("property",propertyFileName);
                        graphName        = commandLine.getOptionValue("graph",graphName);
                        long limit       = Long.parseLong(commandLine.getOptionValue("limit",(new Long(-1)).toString()));
                        GraphStat object = new GraphStat(graphName,propertyFileName,limit);
                        GraphDatabase graph = object.getDatabase();
                        com.infinitegraph.Transaction transaction = graph.beginTransaction(AccessMode.READ);
                        object.analyze(graph);
                        object.writeVertexType(graph);
                        object.writeEdgeType(graph);
                        object.writeEdgeDistribution(graph);
                        transaction.commit();
                        graph.close();                        
                    }
        }
        catch (ParseException e)
        {
            System.out.println(e.toString());
            GraphStat.ShowHelp(options);
        }
        catch (com.infinitegraph.ConfigurationException e)
        {     
            throw new RuntimeException(e);
        }

    }
    
}    

