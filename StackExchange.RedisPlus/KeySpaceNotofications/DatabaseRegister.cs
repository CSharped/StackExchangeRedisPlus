using System;
using System.Collections.Generic;
using Microsoft.Extensions.Caching.Memory;
using StackExchange.Redis;
using StackExchange.RedisPlus.MemoryCache;
using StackExchange.RedisPlus.MemoryCache.Types;
using StackExchange.RedisPlus.MemoryCache.Types.SortedSet;

namespace StackExchange.RedisPlus.KeySpaceNotofications
{
    internal sealed class DatabaseRegister : IDisposable
    {
        internal static DatabaseRegister Instance = new DatabaseRegister();

        private Dictionary<string, DatabaseInstanceData> dbData = new Dictionary<string, DatabaseInstanceData>();
        private static readonly object _lockObj = new object();

        private DatabaseRegister()
        { }

        internal void RemoveInstanceData(string dbIdentifier)
        {
            lock (_lockObj)
            {
                if (dbData.ContainsKey(dbIdentifier))
                {
                    dbData.Remove(dbIdentifier);
                }
            }
        }

        internal DatabaseInstanceData GetDatabaseInstanceData(string dbIdentifier, IDatabase redisDb,IObjMemCache cache)
        {
            //Check if this db is already registered, and register it for notifications if necessary
            lock (_lockObj)
            {
                if (!dbData.ContainsKey(dbIdentifier))
                {
                    dbData.Add(dbIdentifier, new DatabaseInstanceData(redisDb,cache));
                }
            }

            return dbData[dbIdentifier];
        }

        public void Dispose()
        {
            foreach(var db in dbData)
            {
                db.Value.Dispose();
            }

            dbData = new Dictionary<string, DatabaseInstanceData>();
        }
    }

    /// <summary>
    /// Holds details for each Redis database.
    /// </summary>
    internal class DatabaseInstanceData : IDisposable
    {
        /// <summary>
        /// The notification listener to handle keyspace notifications
        /// </summary>
        public NotificationListener Listener { get; private set; }

        /// <summary>
        /// Memory cache for this database
        /// </summary>
        public IObjMemCache MemoryCache { get; private set; }

        internal MemoryStrings MemoryStrings { get; private set; }
        internal MemoryHashes MemoryHashes { get; private set; }
        internal MemorySets MemorySets { get; private set; }
        internal MemorySortedSet MemorySortedSets { get; private set; }

        internal DatabaseInstanceData(IDatabase redisDb,IObjMemCache cache)
        {
            MemoryCache = cache;
            MemoryStrings = new MemoryStrings(MemoryCache);
            MemoryHashes = new MemoryHashes(MemoryCache);
            MemorySets = new MemorySets(MemoryCache);
            MemorySortedSets = new MemorySortedSet(MemoryCache);

            //If we have access to a redis instance, then listen to it for notifications
            if (redisDb != null)
            {
                Listener = new NotificationListener(redisDb.Multiplexer);

                //Connect the memory cache to the listener. Its data will be updated when keyspace events occur.
                Listener.HandleKeyspaceEvents(this);
            }
        }
        
        public void Dispose()
        {
            Listener.Dispose();
            MemoryCache.Dispose();
        }
    }
}
