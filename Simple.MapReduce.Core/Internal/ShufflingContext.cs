namespace Simple.MapReduce.Core.Internal
{
    internal class ShufflingContext<TKEY, TVALUE> where TKEY : notnull
    {
        private readonly Dictionary<TKEY, IList<TVALUE>> _values;
        public ShufflingContext()
        {
            _values = new Dictionary<TKEY, IList<TVALUE>>();
        }

        public void AddKeyValue(TKEY key, IList<TVALUE> values)
        {
            _values.Add(key, values);
        }

        public IEnumerable<ReduceContext<TKEY, TVALUE, TKEYOUT, TVALUEOUT>> GetReducerContexts<TKEYOUT, TVALUEOUT>()
        {
            return _values.Select(x => new ReduceContext<TKEY, TVALUE, TKEYOUT, TVALUEOUT>(x.Key, x.Value));
        }
    }
}
