using System;
using System.Collections.Generic;
using Microsoft.Extensions.Caching.Memory;
using StackExchange.Redis;

namespace StackExchange.RedisPlus.MemoryCache
{
    internal sealed class ObjMemCache : IObjMemCache
    {
        private static readonly object _lockObj = new object();

        //object storage
        private readonly IMemoryCache _cache;
        
        //When you add an item to MemoryCache with a specific TTL, you can't retrieve it again.
        //So, we store them separately.
        private Dictionary<string, DateTimeOffset?> _ttls = new Dictionary<string, DateTimeOffset?>();

        public ObjMemCache(IMemoryCache cache)
        {
            _cache = cache;
        }

        //internal ObjMemCache()
        //{
        //    Flush();
        //}
        
        public bool ContainsKey(string key)
        {
            var value =  _cache.Get(key);
            if (value!=null)
            {
                return true;
            }
            return false;
        }

        public void Update(string key, object o)
        {
            if(ContainsKey(key))
            {
                var expiry = GetExpiry(key);
                TimeSpan? expiryTimespan = expiry.HasValue ? expiry.Value : null;
                Add(key, o, expiryTimespan, When.Always);
            }
        }
        
        public void Add(string key, object o, TimeSpan? expiry, When when)
        {
            lock(_lockObj)
            {
                var value = _cache.Get(key);
                bool exists = value != null;
                
                if (when == When.Exists && !exists)
                {
                    return;
                }

                if (when == When.NotExists && exists)
                {
                    return;
                }

                _cache.Remove(key);
               
                if (expiry.HasValue && expiry.Value != default(TimeSpan))
                {
                    //Store the ttl separately
                    //_ttls[key] = policy.AbsoluteExpiration = DateTime.UtcNow.Add(expiry.Value);
                }
                else
                {
                    _ttls[key] = null;
                }

                System.Diagnostics.Debug.WriteLine("Adding key to mem cache: " + key);
                _cache.Set(key, o,new MemoryCacheEntryOptions());
            }
        }

        public ValOrRefNullable<T> Get<T>(string key)
        {
            lock (_lockObj)
            {
                var value = _cache.Get(key);
                if (value!=null)
                {
                    System.Diagnostics.Debug.WriteLine("Mem cache hit: " + key);
                    T result = (T)value;
                    return new ValOrRefNullable<T>(result);
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine("Mem cache miss: " + key);
                }
            }

            return new ValOrRefNullable<T>();
        }

        /// <summary>
        /// Removes the stored TTL. Note that the key will still expire after the same timespan, it just won't be returned.
        /// To properly update the TTL, call Expire.
        /// </summary>
        public void ClearTimeToLive(RedisKey key)
        {
            if(_ttls.ContainsKey(key))
            {
                _ttls.Remove(key);
            }
        }

        public ValOrRefNullable<TimeSpan?> GetExpiry(string key)
        {
            if(_ttls.ContainsKey(key))
            {
                //There is a TTL stored. Is it null?
                if (_ttls[key].HasValue)
                {
                    //Return it as a timespan
                    return new ValOrRefNullable<TimeSpan?>(_ttls[key].Value.Subtract(DateTime.UtcNow));
                }
                else
                {
                    //There is a null TTL stored (ie, we know it's a non-expiring key)
                    return new ValOrRefNullable<TimeSpan?>(null);
                }
            }
            else
            {
                //There is no TTL stored - we would have to go to redis to get it.
                return new ValOrRefNullable<TimeSpan?>();
            }
        }

        public bool Expire(string key, DateTimeOffset? expiry)
        {
            TimeSpan? diff = null;

            if(expiry.HasValue && expiry != default(DateTimeOffset))
            {
                diff = expiry.Value.Subtract(DateTime.UtcNow);
            }

            return Expire(key, diff);
        }

        public bool Expire(string key, TimeSpan? expiry)
        {
            var o = Get<object>(key);
            if(o.HasValue)
            {
                if (expiry == null || expiry.Value.TotalMilliseconds > 0)
                {
                    //Re-add with the given expiry
                    Add(key, o.Value, expiry, When.Always);
                }
                else
                {
                    Remove(new[] { key });
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        public bool RenameKey(string keyFrom, string keyTo)
        {
            lock (_lockObj)
            {
                var valueFromCache = _cache.Get(keyFrom);
                if (valueFromCache != null && keyFrom != keyTo)
                {
                    System.Diagnostics.Debug.WriteLine("RenameKey: from " + keyFrom + " to " + keyTo);

                    var value = _cache.Get(keyFrom);
                    _cache.Remove(keyFrom);
                    
                    //Get the existing TTL
                    TimeSpan? ttl = null;
                    if(_ttls.ContainsKey(keyFrom))
                    {
                        if (_ttls[keyFrom].HasValue)
                        {
                            ttl = _ttls[keyFrom].Value.Subtract(DateTime.UtcNow);
                        }

                        //Remove the existing TTL
                        _ttls.Remove(keyFrom);
                    }
                    
                    Add(keyTo, value, ttl, When.Always);

                    return true;  
                }

                return false;
            }
        }

        public long Remove(string[] keys)
        {
            long result = 0;
            lock (_lockObj)
            {
                foreach (string key in keys)
                {
                    var value = _cache.Get(key);
                    if (!string.IsNullOrEmpty(key) && value!=null)
                    {
                        System.Diagnostics.Debug.WriteLine("Removing key from memcache: " + key);
                        _cache.Remove(key);
                        
                        if (_ttls.ContainsKey(key))
                        {
                            _ttls.Remove(key);
                        }

                        result++;
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Clears the cache.
        /// </summary>
        //public void Flush()
        //{
        //    lock (_lockObj)
        //    {
        //        if(_cache != null)
        //        {
        //            _cache.Dispose();
        //            _ttls.Clear();
        //        }

        //        _cache = new IMemoryCache();
        //    }
        //}

        public void Dispose()
        {
            _cache.Dispose();
        }
    }
}
