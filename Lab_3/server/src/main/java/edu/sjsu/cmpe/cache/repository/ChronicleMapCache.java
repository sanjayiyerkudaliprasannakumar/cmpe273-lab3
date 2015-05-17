package edu.sjsu.cmpe.cache.repository;

import edu.sjsu.cmpe.cache.domain.Entry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ChronicleMapCache implements CacheInterface{

    ChronicleMapBuilder<Long, Entry> builder;
    ChronicleMap<Long, Entry> map;

    public ChronicleMapCache(String serverUrl){
        try {
        	String serverName = extractServerName(serverUrl);
        	String fileName = getFileName(serverName);
        	File file = new File(fileName);
            builder = ChronicleMapBuilder.of(Long.class, Entry.class);
            map = builder.createPersistedTo(file);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private String getFileName(String serverName){
    	if(serverName.equals("server_A"))
    		return "datFile_A.dat";
    	else if(serverName.equals("server_B"))
    		return "datFile_B.dat";
    	else if(serverName.equals("server_C"))
    		return "datFile_C.dat";
    	throw new IllegalArgumentException("Incorrect serverName :" + serverName);
    }
    
    private String extractServerName (String serverName){
    	System.out.println("Server Name: " + serverName);
    	String[] split = serverName.split("/");
    	System.out.println("split size: " + split.length);
    	String serverSplit = split[1];
    	String[] split2 = serverSplit.split("_");
    	String finalServerName = split2[0] + "_" + split2[1];
    	System.out.println("finalServerName: "  + finalServerName);
    	return finalServerName;
     }

    @Override
    public Entry save(Entry newEntry){
        checkNotNull(newEntry, "newEntry instance must not be null");
        map.putIfAbsent(newEntry.getKey(),newEntry);
        //map.putIfAbsent(newEntry.getKey(),newEntry);
        return newEntry;
    }

    @Override
    public Entry get(Long key) {

        checkArgument(key > 0,
                "Key was %s but expected greater than zero value", key);
        return map.get(key);
    }

    @Override
    public List<Entry> getAll() {
        return new ArrayList<Entry>(map.values());

    }



}