# StackExchangeRedisPlus
A Wrapper around StackExchangeRedis library for ASP.NET Core Applications

Why and How :- 

StackExchange.Redis is a client library which helps us to connect to Azure Redis.
Redis helps us improve the performance of the App drastically by saving the time required to retreive data from an RDBMS like SQL Server 
which has millions of rows. 

While Redis helps us to retreive data faster, but an App using Redis will also have to face the problem of network latecny sometimes, which
will not give you the desired performance results from the App.

So this library helps you to also tacle network latency by storing the data with in the Server in InMemory!!!. At the same time will utilize
KeySpace Notifications from Redis to Clear InMemory Cache when required.

Usage:- 

Instead of direclty using StackExchange.Redis use it as below 

            var dataBase = _connectionMultiplexer.GetDatabase();
            var redisL1Database = new RedisL1Database(dataBase, _cache);
            await redisL1Database.StringSetAsync(key, value);
            
And on Azure enable KeySpace Notifications.
