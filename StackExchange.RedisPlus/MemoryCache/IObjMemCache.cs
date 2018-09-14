using System;
using StackExchange.Redis;

namespace StackExchange.RedisPlus.MemoryCache
{
    internal interface IObjMemCache
    {
        bool ContainsKey(string key);
        void Update(string key, object o);
        void Add(string key, object o, TimeSpan? expiry, When when);
        ValOrRefNullable<T> Get<T>(string key);
        ValOrRefNullable<TimeSpan?> GetExpiry(string key);
        bool Expire(string key, DateTimeOffset? expiry);
        bool Expire(string key, TimeSpan? expiry);
        bool RenameKey(string keyFrom, string keyTo);
        long Remove(string[] keys);
        void ClearTimeToLive(RedisKey key);

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
        void Dispose();
    }
}