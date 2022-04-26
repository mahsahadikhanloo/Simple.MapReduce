namespace Simple.MapReduce.Core.Internal
{
    public enum MapperContextStatus { Init, Runing, Done, Error, Canceld }
    public class MapContext<TKEYOUT, TVALUEOUT>
    {
        private readonly IEnumerable<string> _input;
        private readonly IList<(TKEYOUT, TVALUEOUT)> _output;
        private Exception _exception;
        private MapperContextStatus _status;
        public MapContext(IEnumerable<string> records)
        {
            _input = records;
            _output = new List<(TKEYOUT, TVALUEOUT)>();
            _status = MapperContextStatus.Init;


        }
        public IEnumerable<string> Inputs => _input;

        public void Write(TKEYOUT key, TVALUEOUT value)
        {
            _status = MapperContextStatus.Runing;
            _output.Add((key, value));
        }

        public void CatchException(Exception exception)
        {
            _exception = exception;
            _status = MapperContextStatus.Error;
        }

        public void Done()
        {
            _status = MapperContextStatus.Done;
        }

        public void Canceld()
        {
            _status = MapperContextStatus.Canceld;
        }

        public MapperContextStatus Satus => _status;
        public IList<(TKEYOUT Key, TVALUEOUT Value)> Result => _status == MapperContextStatus.Done ? _output : null!;
    }
}
