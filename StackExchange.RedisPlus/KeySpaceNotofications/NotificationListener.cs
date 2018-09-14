﻿using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
using StackExchange.RedisPlus.Notifications;

namespace StackExchange.RedisPlus.KeySpaceNotofications
{
    internal class NotificationListener : IDisposable
    {
        private static readonly string _keyspace = "__keyspace@0__:";
        private static readonly string _keyspaceDetail = "__keyspace_detailed@0__:";

        private List<DatabaseInstanceData> _databases = new List<DatabaseInstanceData>();
        private ISubscriber _subscriber;
        
        internal bool Paused { get; set; }
        
        internal NotificationListener(ConnectionMultiplexer connection)
        {
            _subscriber = connection.GetSubscriber();

            //Listen for standard redis keyspace events
            _subscriber.Subscribe(_keyspace + "*", (channel, value) =>
            {
                if (!Paused)
                {
                    string key = ((string)channel).Replace(_keyspace, "");
                    foreach (DatabaseInstanceData dbData in _databases)
                    {
                        HandleKeyspaceEvent(dbData, key, value);
                    }
                }
            });

            //Listen for advanced keyspace events
            _subscriber.Subscribe(_keyspaceDetail + "*", (channel, value) =>
            {
                if (!Paused)
                {
                    string machine = ((string)value).Split(':').First();

                    //Only listen to events caused by other redis clients
                    if (machine != ProcessId.GetCurrent())
                    {
                        string key = ((string)channel).Replace(_keyspaceDetail, "");

                        string eventType = ((string)value).Substring(machine.Length + 1);
                        foreach (DatabaseInstanceData dbData in _databases)
                        {
                            HandleKeyspaceDetailEvent(dbData, key, machine, eventType);
                        }
                    }
                }
            });
        }
        
        public void Dispose()
        {
            _subscriber.Unsubscribe(_keyspace + "*");
        }

        internal void HandleKeyspaceEvents(DatabaseInstanceData dbData)
        {
            _databases.Add(dbData);
        }

        private void HandleKeyspaceDetailEvent(DatabaseInstanceData dbData, string key, string machine, string eventType)
        {
            var keySpaceDetails = new Dictionary<string,string>();
			keySpaceDetails.Add("Machine",machine);
	        keySpaceDetails.Add("key", key);
	        keySpaceDetails.Add("Event", eventType);
			
            string eventName = eventType.Split(':').First();
            string eventArg = "";
            if (eventName.Length < eventType.Length)
            {
                eventArg = eventType.Substring(eventName.Length + 1);
            }

            if (eventName == "hset" || eventName == "hdel" ||
                eventName == "hincr" || eventName == "hincrbyfloat" ||
                eventName == "hdecr" || eventName == "hdecrbyfloat")
            {
                //eventArg is the hash entry name. Since it has changed, remove it.
                dbData.MemoryHashes.Delete(key, new[] { (RedisValue)eventArg });
            }
            else if (eventName == "srem")
            {
                //Removing an item from a set.
                dbData.MemorySets.RemoveByHashCode(key, new[] { eventArg });
            }
            else if (eventName == "zadd")
            {
                //An item is added to a sorted set. We should remove it from its current location if it's already there.
                int hashCode;
                if (int.TryParse(eventArg, out hashCode))
                {
                    dbData.MemorySortedSets.RemoveByHashCode(key, hashCode);
                }
            }
            else if (eventName == "zrem" || eventName == "zincr" || eventName == "zdecr")
            {
                //An item is removed from a sorted set.
                int hashCode;
                if (int.TryParse(eventArg, out hashCode))
                {
                    dbData.MemorySortedSets.RemoveByHashCode(key, hashCode);
                }
            }
            else if (eventName == "zremrangebyscore")
            {
                if (!string.IsNullOrEmpty(eventArg))
                {
                    string[] scores = eventArg.Split('-');
                    if(scores.Length == 3)
                    {
                        double start, stop;
                        int exclude;
                        if(double.TryParse(scores[0], out start) && double.TryParse(scores[1], out stop) && int.TryParse(scores[2], out exclude))
                        {
                            dbData.MemorySortedSets.DeleteByScore(key, start, stop, (Exclude)exclude);
                        }
                    }
                }
            }
            else if (eventName == "del")
            {
                //A key was removed
                dbData.MemoryCache.Remove(new[] { key });
            }
            else if (eventName == "expire")
            {
                //The TTL has changed - clear it in memory
                dbData.MemoryCache.ClearTimeToLive(key);
            }
            else if (eventName == "rename_key")
            {
                //the arg contains the new key
                if (!string.IsNullOrEmpty(eventArg))
                {
                    dbData.MemoryCache.RenameKey(key, eventArg);
                }
            }
            else if (eventName == "set" /* Setting a string */)
            {
                //A key has been set by another client. If it exists in memory, it is probably now outdated.
				
                dbData.MemoryCache.Remove(new[] { key });
	            
			}
            else if (eventName == "setbit" || eventName == "setrange" ||
                    eventName == "incrby" || eventName == "incrbyfloat" ||
                    eventName == "decrby" || eventName == "decrbyfloat" ||
                    eventName == "append")
            {
                //Many string operations are not performed in-memory, so the key needs to be invalidated and we go back to redis for the result.
                dbData.MemoryCache.Remove(new[] { key });
            }
            else if (eventName == "zremrangebyrank" || eventName == "zremrangebylex")
            {
                //Many sorted set operations are not performed in-memory, so the key needs to be invalidated and we go back to redis for the result.
                dbData.MemoryCache.Remove(new[] { key });
            }
        }

        /// <summary>
        /// Reads the key/value and updates the database with the relevant value
        /// </summary>
        private void HandleKeyspaceEvent(DatabaseInstanceData dbData, string key, string value)
        {
           
            if(value == "expired")
            {
                //A key has expired. Sometimes the expiry is performed in-memory, so the key may have already been removed.
                //It's also possible that the expiry is performed in redis and not in memory, so we listen for this event.
                dbData.MemoryCache.Remove(new[] { key });
				
            }
        }
    }
}
