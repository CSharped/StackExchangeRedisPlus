using System;
using StackExchange.Redis;

namespace StackExchange.RedisPlus.Notifications
{
    public static class RedisValueHashCode
    {
        public static int GetStableHashCode(RedisValue value)
        {
            if (value.IsNull)
            {
                return 0;
            }
            else if (value.IsInteger)
            {
                //Get the integer bytes
                byte[] bytes = BitConverter.GetBytes((int)value);
                return ((RedisValue)bytes).GetHashCode();
            }
            else
            {
                return value.GetHashCode(); //Use redis GetHashCode which is stable for non-ints
            }
        }
    }
}
