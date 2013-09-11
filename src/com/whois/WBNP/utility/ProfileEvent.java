package com.objectivity.ig.utility;

import java.util.*;
import java.io.*;


import java.lang.Math;
import java.util.*;
import java.lang.management.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.json.simple.*;

public class ProfileEvent
{
    private long startTime = 0;
    private long stopTime = 0;
    private boolean running = false;
    private String fileName = null;
       
    public ProfileEvent(String fileName)
    {
        this.fileName = fileName;
    }
    
    public void start()
    {
        this.startTime = System.currentTimeMillis();
        this.running = true;
    }
    
    public void stop()
    {
        this.stopTime = System.currentTimeMillis();
        this.running = false;
    }
        
    public synchronized void save(JSONObject data) throws Exception
    {
        PrintWriter writer =  new PrintWriter(new FileWriter(this.fileName,true));
        writer.println(this.toJSON(data).toString());
        writer.flush();
        writer.close();
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSON(JSONObject data)
    {
        JSONObject object = new JSONObject();
        object.put("time",this.getElapsedTime());
        if(data != null)
            object.put("data",data);
        return object;
    }
    
    public double getElapsedTime()
    {
        double elapsed;
        if (running)
        {
            elapsed = (System.currentTimeMillis() - startTime);
        }
        else
        {
            elapsed = (stopTime - startTime);
        }
        return elapsed;
    }
    
    public double getElapsedTimeSecs()
    {
        long elapsed;
        if (running)
        {
            elapsed = ((System.currentTimeMillis() - startTime) / 1000);
        }
        else
        {
            elapsed = ((stopTime - startTime) / 1000);
        }
        return elapsed;
    }


    private static MemoryMXBean memorymxBean;
    static
    {
        ProfileEvent.memorymxBean = ManagementFactory.getMemoryMXBean();
    }
    public static double GetMemoryUsageInit()
    {
        return (ProfileEvent.memorymxBean.getHeapMemoryUsage().getInit());
    }
    
    public static double GetMemoryUsageUsed()
    {
        return (ProfileEvent.memorymxBean.getHeapMemoryUsage().getUsed());
    }
    
    public static double GetMemoryUsageCommitted()
    {
        return (ProfileEvent.memorymxBean.getHeapMemoryUsage().getCommitted());
    }
    
    public static double GetMemoryUsageMax()
    {
        return (ProfileEvent.memorymxBean.getHeapMemoryUsage().getMax());
    }

}
