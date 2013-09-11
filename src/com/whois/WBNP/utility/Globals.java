package com.objectivity.ig.utility;
import java.util.*;

public class Globals
{
    static String profileTag = null;
    static int verboseLevel = 0;

    public static void setProfileTag(String value)
    {
         Globals.profileTag = value;
    }

    public static String getProfileTag()
    {
        return Globals.profileTag;
    }

    public static String getProfileFileName()
    {
        if(Globals.profileTag != null)
            return String.format("%s.benchmark.profile",Globals.profileTag);
        return "benchmark.profile";
    }

    public static String getProfileEventFileName()
    {
        if(Globals.profileTag != null)
            return String.format("%s.benchmark.event",Globals.profileTag);
        return "benchmark.event";
    }

    public static void SetVerboseLevel(int value)
    {
	Globals.verboseLevel = value;
    }
    
    public static int GetVerboseLevel()
    {
	return Globals.verboseLevel;
    }
    
}

