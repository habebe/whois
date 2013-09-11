package com.objectivity.ig.utility;

import java.util.*;
import java.io.*;


import java.lang.Math;
import java.util.*;
import java.lang.management.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.json.simple.*;

public class Profile extends ProfileEvent
{
    private int numberOfThreads;
    private int txSize;
    public Profile(String fileName,int numberOfThreads,int txSize)
    {
        super(fileName);
        this.numberOfThreads = numberOfThreads;
        this.txSize = txSize;
    }
    
    @SuppressWarnings("unchecked")
    public JSONObject toJSON(JSONObject data)
    {
        JSONObject object = new JSONObject();
        object.put("time",this.getElapsedTime());
        object.put("numberOfThreads",this.numberOfThreads);
        object.put("txsize",this.txSize);
        object.put("os",System.getProperty("os.name").replace(' ','_').toLowerCase());
        object.put("memInit",ProfileEvent.GetMemoryUsageInit());
        object.put("memUsed",ProfileEvent.GetMemoryUsageUsed());
        object.put("memCommitted",ProfileEvent.GetMemoryUsageCommitted());
        object.put("memMax",ProfileEvent.GetMemoryUsageMax());
        if(data != null)
        {
            object.put("data",data);
        }
        return object;
    }
    
}
