/*
 * Copyright (C) 2015 Couchbase Inc., the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.spring.cache;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.*;

import com.couchbase.client.java.Bucket;
import org.springframework.util.CollectionUtils;

/**
 * The {@link CouchbaseCacheManager} orchestrates {@link CouchbaseCache} instances.
 * 
 * Since more than one current {@link Bucket} connection can be used for caching, the
 * {@link CouchbaseCacheManager} orchestrates and handles them for the Spring {@link Cache} abstraction layer.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Konrad Król
 * @author Stephane Nicoll
 */
public class CouchbaseCacheManager extends AbstractCacheManager {

  private final Bucket defaultBucket;

  private boolean dynamic = true;

  private int defaultTtl = 0;

  private Map<String, Bucket> clients = new LinkedHashMap<String, Bucket>();

  public CouchbaseCacheManager(Bucket defaultBucket, Collection<String> cacheNames) {
    this.defaultBucket = defaultBucket;
    Set<String> names = CollectionUtils.isEmpty(cacheNames) ? Collections.<String> emptySet()
            : new HashSet<String>(cacheNames);
    this.dynamic = names.isEmpty();
    for (String name : names) {
      clients.put(name, defaultBucket);
    }
  }

  public CouchbaseCacheManager(Bucket defaultBucket) {
    this(defaultBucket, Collections.<String>emptyList());
  }

  /**
   * Set the default Time To Live value for all caches created from that point
   * forward.
   */
  public void setDefaultTtl(int defaultTtl) {
    this.defaultTtl = defaultTtl;
  }

  public void addCache(String cacheName, int ttl) {
    addCache(cacheName, ttl, defaultBucket);
  }

  public void addCache(String cacheName, int ttl, Bucket bucket) {
    addCache(createCache(cacheName, ttl, bucket));
  }

  @Override
  protected Cache getMissingCache(String name) {
    if (this.dynamic) {
      return createCache(name, defaultTtl, defaultBucket);
    }
    return null;
  }

  /**
   * Populates all caches.
   *
   * @return a collection of loaded caches.
   */
  @Override
  protected final Collection<? extends Cache> loadCaches() {
    Collection<Cache> caches = new LinkedHashSet<Cache>();

    for (Map.Entry<String, Bucket> cache : clients.entrySet()) {
      caches.add(createCache(cache.getKey(),defaultTtl,  cache.getValue()));
    }

    return caches;
  }

  private CouchbaseCache createCache(String name, Integer ttl, Bucket bucket) {
    return new CouchbaseCache(name, bucket, ttl);
  }

}
