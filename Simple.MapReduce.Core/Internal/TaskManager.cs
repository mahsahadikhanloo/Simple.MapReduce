using System.Collections.Concurrent;
namespace Simple.MapReduce.Core.Internal
{
    internal class TaskManager<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>
    {
        private readonly CancellationToken _cancellationToken;
        public TaskManager(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
        }

        public void RunMapPhase(Type mapperType, IEnumerable<MapContext<TKEYIN, TVALUEIN>> contexts)
        {
            Logger.Info("[{0}]:: map phase is starting...", " MAP ");
            var tasks = new List<Task>();

            var taskNumber = 0;
            foreach (var context in contexts)
            {
                taskNumber++;
                var currentNumber = taskNumber;
                var newTask = new Task(() =>
                {
                    Logger.Debug("          Task #[{0}] is starting...", currentNumber);
                    var mapper = Activator.CreateInstance(mapperType) as Mapper<TKEYIN, TVALUEIN>;
                    mapper?.Run(context, _cancellationToken);
                    Logger.Debug("          Task #[{0}] is completed.", currentNumber);
                }, _cancellationToken);
                tasks.Add(newTask);
            }
            foreach (var task in tasks)
            {
                task.Start();
            }
            Task.WaitAll(tasks.ToArray());
            Logger.Info("[{0}]:: map phase is completed", " MAP ");
        }

        public IEnumerable<(TKEYOUT, TVALUEOUT)> RunReducePahse(Type reducerType, IEnumerable<ShufflingContext<TKEYIN, TVALUEIN>> contexts)
        {
            Logger.Info("[{0}]:: reducing phase is starting...", "RDUCE");
            var tasks = new List<Task>();
            var output = new BlockingCollection<(TKEYOUT, TVALUEOUT)>();
            var taskNumber = 0;
            foreach (var context in contexts)
            {
                taskNumber++;
                var currentNumber = taskNumber;
                tasks.Add(new Task(() =>
                {
                    Logger.Debug("          Task #[{0}] is starting...", currentNumber);
                    foreach (var reducerContext in context.GetReducerContexts<TKEYOUT, TVALUEOUT>())
                    {
                        var reducer = Activator.CreateInstance(reducerType) as Reducer<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>;
                        reducer?.Run(reducerContext, _cancellationToken);
                        Logger.Debug("          reduce task #[{0}] is completed for {1}", currentNumber, reducerContext.Result.key!);
                        output.Add(reducerContext.Result);
                    }
                    Logger.Debug("          Task #[{0}] is completed.", currentNumber);
                }, _cancellationToken));
            }

            foreach (var task in tasks)
            {
                task.Start();
            }

            Task.WaitAll(tasks.ToArray());
            Logger.Info("[{0}]:: reducing phase is completed.", "RDUCE");
            return output;
        }
    }
}
