using Simple.MapReduce.Core.Internal;

namespace Simple.MapReduce.Core
{
    public class JobBuilder
    {
        private string _jobName;
        private string _inputFilePath;
        private string _outputFilePath;
        private FileInputFormat _fileInputFormat;
        private FileOutputFormat _fileOutputFormat;
        private Type _mapperType;
        private Type _reducerType;
        public JobBuilder SetJobName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            _jobName = name;
            return this;
        }
        public JobBuilder AddInputPath(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (!File.Exists(path))
                throw new FileNotFoundException(path);

            _inputFilePath = path;
            return this;
        }

        public JobBuilder SetOutputPath(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (File.Exists(path))
                throw new IOException(path);

            _outputFilePath = path;
            return this;
        }

        public JobBuilder SetMapper<T>() where T : new()
        {
            if (typeof(T).IsAssignableFrom(typeof(Mapper<,>)))
                throw new ArgumentException();
            _mapperType = typeof(T);
            return this;
        }

        public JobBuilder SetReducer<T>() where T : new()
        {
            if (typeof(T).IsAssignableFrom(typeof(Reducer<,,,>)))
                throw new ArgumentException();
            _reducerType = typeof(T);
            return this;
        }

        public JobBuilder SetInputFileFormat<T>() where T : FileInputFormat, new()
        {
            _fileInputFormat = new T();
            return this;
        }

        public JobBuilder SetOutputFileFormat<T>() where T : FileOutputFormat, new()
        {
            _fileOutputFormat = new T();
            return this;
        }

        public Job<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> Buid<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>(CancellationToken cancellationToken) where TKEYIN : notnull
        {
            return new Job<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>(
                jobName: _jobName,
                inputPath: _inputFilePath,
                outputPath: _outputFilePath,
                fileInputFormat: _fileInputFormat,
                fileOutputFormat: _fileOutputFormat,
                mapperType: _mapperType,
                reducerType: _reducerType,
                cancellationToken);
        }
    }
}
