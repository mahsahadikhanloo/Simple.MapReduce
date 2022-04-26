using Simple.MapReduce.Core.Internal;

namespace Simple.MapReduce.Core
{
    public class Job<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> where TKEYIN : notnull
    {
        private readonly string _jobName;
        private readonly string _inputPath;
        private readonly string _outputPath;
        private readonly FileInputFormat _fileInputFormat;
        private readonly FileOutputFormat _fileOutputFormat;
        private readonly Type _mapperType;
        private readonly Type _reducerType;
        private readonly Splitter<TKEYIN, TVALUEIN> _splitter;
        private readonly TaskManager<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> _taskManager;
        private readonly Shuffling<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> _shuffling;
        internal Job(
            string jobName,
            string inputPath,
            string outputPath,
            FileInputFormat fileInputFormat,
            FileOutputFormat fileOutputFormat,
            Type mapperType,
            Type reducerType,
            CancellationToken cancellationToken)
        {
            _jobName = jobName;
            _inputPath = inputPath;
            _outputPath = outputPath;
            _fileInputFormat = fileInputFormat;
            _fileOutputFormat = fileOutputFormat;
            _mapperType = mapperType;
            _reducerType = reducerType;
            _splitter = new Splitter<TKEYIN, TVALUEIN>();
            _taskManager = new TaskManager<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>(cancellationToken);
            _shuffling = new Shuffling<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>(cancellationToken);
        }

        public bool Run()
        {
            Logger.Info("Job [{0}] is starting...", _jobName);
            try
            {
                var inputs = ReadInput();
                var mappersContexts = _splitter.RunSplittPhase(inputs);
                _taskManager.RunMapPhase(_mapperType, mappersContexts);
                var shufflingContexts = _shuffling.RunShuffelPhase(_mapperType, mappersContexts);
                var output = _taskManager.RunReducePahse(_reducerType, shufflingContexts);
                WriteOutput(output);
            }
            catch (Exception exception)
            {
                Logger.Error("Error : [{0}]", exception.Message);
                return false;
            }
            Logger.Info("Job [{0}] is completed.", _jobName);
            return true;
        }

        private IEnumerable<string> ReadInput()
        {
            var records = _fileInputFormat.Read(_inputPath);
            if (!records.Any())
                throw new Exception($"{_fileInputFormat.GetType()} did not return any records.");
            Logger.Info("[{0}]:: [{1}] Number of input records loaded.", "INPUT", records.Count());
            return records;
        }

        private bool WriteOutput(IEnumerable<(TKEYOUT, TVALUEOUT)> output)
        {
            Logger.Info("[{0}]:: [{1}] Number of output records is written.", "OUTPT", output.Count());
            return _fileOutputFormat.Write(_outputPath, output);
        }
    }
}
