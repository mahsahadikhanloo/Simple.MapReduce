namespace Simple.MapReduce.Core
{
    public enum ReduceContextStatus { Init, Runing, Done, Error, Canceld }
    public class ReduceContext<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>
    {
        private readonly TKEYIN _key;
        private readonly IEnumerable<TVALUEIN> _values;
        private (TKEYOUT key, TVALUEOUT value) _output;
        private Exception _exception;
        private ReduceContextStatus _status;
        public ReduceContext(TKEYIN key, IEnumerable<TVALUEIN> values)
        {
            _key = key;
            _values = values;
            _status = ReduceContextStatus.Init;
        }
        public (TKEYIN Key, IEnumerable<TVALUEIN> Values) Inputs => (_key, _values);

        public void Write(TKEYOUT key, TVALUEOUT value)
        {
            _status = ReduceContextStatus.Runing;
            _output = (key, value);
        }

        public void CatchException(Exception exception)
        {
            _exception = exception;
            _status = ReduceContextStatus.Error;
        }

        public void Done()
        {
            _status = ReduceContextStatus.Done;
        }

        public void Canceld()
        {
            _status = ReduceContextStatus.Canceld;
        }

        public ReduceContextStatus Satus => _status;
        public (TKEYOUT key, TVALUEOUT value) Result => _status == ReduceContextStatus.Done ? _output : throw new Exception("Invalid reducer output.");
    }
}
