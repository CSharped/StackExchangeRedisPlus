namespace StackExchange.RedisPlus.MemoryCache
{
    internal class ValOrRefNullable<T>
    {
        public bool HasValue { get; private set; }
        public T Value { get; private set; }

        public ValOrRefNullable()
        {
            HasValue = false;
        }

        public ValOrRefNullable(T value)
        {
            HasValue = true;
            Value = value;
        }
    }
}
