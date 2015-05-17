package edu.sjsu.cmpe.cache.client;



import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;

public class Client {
    private final int numberOfReplicas = 3;
	private final SortedMap<String, CacheServiceInterface> circle
            = new TreeMap<String, CacheServiceInterface>();
    private final HashFunction md5 = Hashing.md5();

	public Client(Collection<CacheServiceInterface> nodes) {
		for (CacheServiceInterface node : nodes) {
			add(node);
		}
	}

	public void add(CacheServiceInterface node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String hash = md5.newHasher().putString(node.toString() + i, Charsets.UTF_8).hash().toString();
            circle.put(hash, node);
        }
	}

	public void remove(CacheServiceInterface node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String hash = md5.newHasher().putString(node.toString()+i, Charsets.UTF_8).toString();
            circle.remove(hash);
        }
	}

	public CacheServiceInterface consistentHash(long key) {
		if (circle.isEmpty()) {
			return null;
		}
		String hash = md5.newHasher().putLong(key).hash().toString();
        System.out.println(hash);
		if (!circle.containsKey(hash)) {
			SortedMap<String, CacheServiceInterface> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}

    public CacheServiceInterface rendezvousHash(long key) {
        long maxValue = Long.MIN_VALUE;
        CacheServiceInterface max = null;
        for (CacheServiceInterface node : circle.values()) {
            Long hash = md5.newHasher()
                    .putString(node.toString(), Charsets.UTF_8)
                    .putLong(key)
                    .hash().asLong();
            if (hash > maxValue) {
                max = node;
                maxValue = hash;
            }
        }
        return max;
    }

	public static void main(String[] args) throws Exception {
		System.out.println("Starting the Cache in the Client...");
		CacheServiceInterface cache1 = new DistributedCacheService(
				"http://localhost:3000");
		CacheServiceInterface cache2 = new DistributedCacheService(
				"http://localhost:3001");
		CacheServiceInterface cache3 = new DistributedCacheService(
				"http://localhost:3002");
		ArrayList<CacheServiceInterface> nodes = new ArrayList<CacheServiceInterface>();
		nodes.add(cache1);
		nodes.add(cache2);
		nodes.add(cache3);

		Client client = new Client(nodes);

        HashMap<Long, String> keyvalues = new HashMap<Long, String>();
        keyvalues.put(1l, "a");
        keyvalues.put(2l, "b");
        keyvalues.put(3l, "c");
        keyvalues.put(4l, "d");
        keyvalues.put(5l, "e");
        keyvalues.put(6l, "f");
        keyvalues.put(7l, "g");
        keyvalues.put(8l, "h");
        keyvalues.put(9l, "i");
        keyvalues.put(10l, "j");


        for(Long key : keyvalues.keySet()){
            String value= keyvalues.get(key);
            CacheServiceInterface bucket = client.consistentHash(key);
            bucket.put(key,value);
        }
        for (Long key : keyvalues.keySet()) {
            DistributedCacheService bucket = (DistributedCacheService)client.consistentHash(key);
            String value =bucket.get(key);
            System.out.println(cache.getCacheServerUrl() + ": " + key + "=>" + value);
        }

        System.out.println();

        for(Long key : keyvalues.keySet()){
            String value= keyvalues.get(key);
            CacheServiceInterface bucket = client.rendezvousHash(key);
            bucket.put(key,value);
        }

        for (Long key : keyvalues.keySet()) {
            DistributedCacheService cache = (DistributedCacheService)client.rendezvousHash(key);
            String value =bucket.get(key);
            System.out.println(cache.getCacheServerUrl() + ": " + key + "=>" + value);
        }

	}

}
