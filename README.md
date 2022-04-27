# CloudComputing.MapReduce
My MapReduce implementation for Cloud Computing course work at University of Reading


# Report
## Abstract
In this project I implemented a map-reduce concept from scratch. This concept is one of the core components of the Hadoop framework which is called MapReduce. This component is responsible for computation / data processing.
I implemented key concepts at the MapReduce component such as Job, TaskTracker, Map, Shuffle, and Reduce with a generic approach.
Each Map and Reduce class is an abstract generic class. I create spirited mapper and reducer classes for task one and two while inheriting from those base classes.
I also use C# and Task in dotnet for implementing concurrent computation needed for different phases of MapReduce. My simple hadoop engine has developed in a clean manner and follows object oriented principles as well.
For the first task we need only one map and reduce run, another other hand for the second task we need two sequential maps and reduce run with different criteria.

## Getting started
This section outlines how to build the app and run it locally. To start building the app, download and install the .NET SDK (Software Development Kit). I test the project with .NET 6 x64 on windows successfully. Once you've installed it, open a new command prompt and run the following command:

```shell
cd .\Reading.BigData.Coursework\
dotnet build
```

### First Task
In order to run first task run the following command:

```shell
dotnet run AIRPORT .\Data\AComp_Passenger_data_no_error.csv
```
Then you see the result like the following lines in the console:

```shell
Job [NumberOfFlightsPerAirports] is starting...
[INPUT]:: [500] Number of input records loaded.
[SPLIT]:: splitting phase is starting...
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.   
          create a new map context for [62] number of input records.   
          create a new map context for [62] number of input records.   
          create a new map context for [62] number of input records.   
          create a new map context for [62] number of input records.   
          create a new map context for [62] number of input records.   
          create a new map context for [62] number of input records.   
          create a new map context for [4] number of input records.    
[SPLIT]:: splitting phase is completed with [9] number of map contexts.
[ MAP ]:: map phase is starting...
          Task #[1] is starting...
          Task #[5] is starting...
          Task #[2] is starting...
          Task #[6] is starting...
          Task #[3] is starting...
          Task #[8] is starting...
          Task #[4] is starting...
          Task #[7] is starting...
          Task #[9] is starting...
          Task #[1] is completed.
          Task #[6] is completed.
          Task #[5] is completed.
          Task #[2] is completed.
          Task #[3] is completed.
          Task #[8] is completed.
          Task #[4] is completed.
          Task #[7] is completed.
          Task #[9] is completed.
[ MAP ]:: map phase is completed
[SHFFL]:: shuffling phase is starting...
          [22] number of keys and [500] number of values are combined ans sorted for shuffling.
          create [8] number of shuffling context.
          add [3] number of keys into #[1] context.
          add [3] number of keys into #[2] context.
          add [3] number of keys into #[3] context.
          add [3] number of keys into #[4] context.
          add [3] number of keys into #[5] context.
          add [3] number of keys into #[6] context.
          add [3] number of keys into #[7] context.
          add [1] number of keys into #[8] context.
[SHFFL]:: shuffling phase is completed.
[RDUCE]:: reducing phase is starting...
          Task #[1] is starting...
          Task #[4] is starting...
          Task #[3] is starting...
          Task #[2] is starting...
          Task #[6] is starting...
          Task #[5] is starting...
          Task #[7] is starting...
          Task #[8] is starting...
          reduce task #[7] is completed for MUC
          reduce task #[3] is completed for CLT
          reduce task #[4] is completed for FCO
          reduce task #[8] is completed for PVG
          reduce task #[2] is completed for CAN
          reduce task #[5] is completed for JFK
          reduce task #[6] is completed for LHR
          reduce task #[1] is completed for AMS
          reduce task #[7] is completed for ORD
          reduce task #[4] is completed for HND
          reduce task #[3] is completed for DEN
          Task #[8] is completed.
          reduce task #[2] is completed for CDG
          reduce task #[5] is completed for KUL
          reduce task #[6] is completed for MAD
          reduce task #[1] is completed for ATL
          reduce task #[7] is completed for PEK
          reduce task #[4] is completed for IAH
          reduce task #[3] is completed for DFW
          reduce task #[2] is completed for CGK
          reduce task #[5] is completed for LAS
          reduce task #[6] is completed for MIA
          reduce task #[1] is completed for BKK
          Task #[7] is completed.
          Task #[4] is completed.
          Task #[3] is completed.
          Task #[2] is completed.
          Task #[5] is completed.
          Task #[6] is completed.
          Task #[1] is completed.
[RDUCE]:: reducing phase is completed.
[OUTPT]:: [22] Number of output records is written.
Job [NumberOfFlightsPerAirports] is completed.
```

```shell
cd .\Reading.BigData.Coursework\
dotnet build
```

### Second Task
In order to run second task run the following command:

```shell
dotnet run PASSENGERS .\Data\AComp_Passenger_data_no_error.csv
```
Then you see the result like the following lines in the console:

```shell
Job [PassengersWithHighestNumberOfFlightsStepOne] is starting...
[INPUT]:: [500] Number of input records loaded.
[SPLIT]:: splitting phase is starting...
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [62] number of input records.
          create a new map context for [4] number of input records.
[SPLIT]:: splitting phase is completed with [9] number of map contexts.
[ MAP ]:: map phase is starting...
          Task #[2] is starting...
          Task #[7] is starting...
          Task #[5] is starting...
          Task #[1] is starting...
          Task #[4] is starting...
          Task #[3] is starting...
          Task #[6] is starting...
          Task #[8] is starting...
          Task #[9] is starting...
          Task #[7] is completed.
          Task #[1] is completed.
          Task #[2] is completed.
          Task #[5] is completed.
          Task #[4] is completed.
          Task #[3] is completed.
          Task #[6] is completed.
          Task #[8] is completed.
          Task #[9] is completed.
[ MAP ]:: map phase is completed
[SHFFL]:: shuffling phase is starting...
          [31] number of keys and [500] number of values are combined ans sorted for shuffling.
          create [8] number of shuffling context.
          add [4] number of keys into #[1] context.
          add [4] number of keys into #[2] context.
          add [4] number of keys into #[3] context.
          add [4] number of keys into #[4] context.
          add [4] number of keys into #[5] context.
          add [4] number of keys into #[6] context.
          add [4] number of keys into #[7] context.
          add [3] number of keys into #[8] context.
[SHFFL]:: shuffling phase is completed.
[RDUCE]:: reducing phase is starting...
          Task #[7] is starting...
          Task #[3] is starting...
          Task #[6] is starting...
          Task #[8] is starting...
          Task #[1] is starting...
          Task #[4] is starting...
          Task #[2] is starting...
          Task #[5] is starting...
          reduce task #[4] is completed for JJM4724RF7
          reduce task #[2] is completed for CYJ0225CH1
          reduce task #[7] is completed for UMH6360YP0
          reduce task #[1] is completed for BWI0520BG6
          reduce task #[6] is completed for PUD8209OG3
          reduce task #[8] is completed for WYU2010YH8
          reduce task #[5] is completed for ONL0812DH1
          reduce task #[3] is completed for HCA3158QA6
          reduce task #[4] is completed for KKP5277HZ7
          reduce task #[7] is completed for VZY2993ME1
          reduce task #[6] is completed for SJD8775RZ4
          reduce task #[2] is completed for DAZ3029XA0
          reduce task #[8] is completed for XFG5747ZT9
          reduce task #[1] is completed for CDC0302NN5
          reduce task #[5] is completed for PAJ3974RK1
          reduce task #[3] is completed for HGO4350KK1
          reduce task #[4] is completed for LLZ3798PE3
          reduce task #[7] is completed for WBE6935NU3
          reduce task #[6] is completed for SPR4484HA6
          reduce task #[2] is completed for EDV2089LK5
          reduce task #[8] is completed for YMH6360YP0
          reduce task #[1] is completed for CKZ3132BR4
          reduce task #[5] is completed for PIT2755XC1
          reduce task #[3] is completed for IEG9308EA5
          reduce task #[4] is completed for MXU9187YC7
          reduce task #[7] is completed for WTC9125IE5
          reduce task #[6] is completed for UES9151GS5
          reduce task #[2] is completed for EZC9678QI6
          Task #[8] is completed.
          reduce task #[1] is completed for CXN7304ER2
          reduce task #[5] is completed for POP2875LH3
          reduce task #[3] is completed for JBE2302VO4
          Task #[4] is completed.
          Task #[7] is completed.
          Task #[6] is completed.
          Task #[2] is completed.
          Task #[1] is completed.
          Task #[5] is completed.
          Task #[3] is completed.
[RDUCE]:: reducing phase is completed.
[OUTPT]:: [31] Number of output records is written.
Job [PassengersWithHighestNumberOfFlightsStepOne] is completed.
Job [PassengersWithHighestNumberOfFlightsStepTwo] is starting...
[INPUT]:: [32] Number of input records loaded.
[SPLIT]:: splitting phase is starting...
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [4] number of input records.
          create a new map context for [0] number of input records.
[SPLIT]:: splitting phase is completed with [9] number of map contexts.
[ MAP ]:: map phase is starting...
          Task #[3] is starting...
          Task #[4] is starting...
          Task #[2] is starting...
          Task #[5] is starting...
          Task #[6] is starting...
          Task #[1] is starting...
          Task #[7] is starting...
          Task #[8] is starting...
          Task #[9] is starting...
          Task #[6] is completed.
          Task #[3] is completed.
          Task #[5] is completed.
          Task #[4] is completed.
          Task #[1] is completed.
          Task #[2] is completed.
          Task #[7] is completed.
          Task #[8] is completed.
          Task #[9] is completed.
[ MAP ]:: map phase is completed
[SHFFL]:: shuffling phase is starting...
          [1] number of keys and [31] number of values are combined ans sorted for shuffling.
          create [8] number of shuffling context.
          add [1] number of keys into #[1] context.
          add [0] number of keys into #[2] context.
          add [0] number of keys into #[3] context.
          add [0] number of keys into #[4] context.
          add [0] number of keys into #[5] context.
          add [0] number of keys into #[6] context.
          add [0] number of keys into #[7] context.
          add [0] number of keys into #[8] context.
[SHFFL]:: shuffling phase is completed.
[RDUCE]:: reducing phase is starting...
          Task #[1] is starting...
          Task #[2] is starting...
          Task #[3] is starting...
          Task #[5] is starting...
          Task #[4] is starting...
          Task #[6] is starting...
          Task #[7] is starting...
          Task #[8] is starting...
          Task #[2] is completed.
          Task #[3] is completed.
          Task #[5] is completed.
          reduce task #[1] is completed for UES9151GS5
          Task #[4] is completed.
          Task #[6] is completed.
          Task #[7] is completed.
          Task #[8] is completed.
          Task #[1] is completed.
[RDUCE]:: reducing phase is completed.
[OUTPT]:: [1] Number of output records is written.
Job [PassengersWithHighestNumberOfFlightsStepTwo] is completed.
```

## MapReduce concept
# Job
A Job in the context of Hadoop MapReduce is the unit of work to be performed as requested by the client / user. The information associated with the Job includes the data to be processed (input data), MapReduce logic / program / algorithm, and any other relevant configuration information necessary to execute the Job.

In this project for creating a new job, I implemented a JobBuilder class. In this class we can set different configurations that we need for creating a new valid job. In the following code block you can see, how we can create a new job by JobBuilder:
After creating a new job instance with JobBuilder we need to run the created job. At the Run method I implemented the main behavior of MapReduce framework. Here we have different phases where Job will run them in a proper order and handle all the results and forward the result of the previous phase to the next phase until we reach the final phase and write the final result to a different store.

```c#
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
```

# Input
This is the input data/file to be processed. In this step, the sample file is input to MapReduce. We can use different types of input only by creating a new class which is inherited from FileInputFormat.
For example, I implement a simple FileInputFormat for text files. This TextInputFormat just reads each file line by line. We can implement another type of FileInputFormat such as a CSV reader.

```c#
private IEnumerable<string> ReadInput()
        {
            var records = _fileInputFormat.Read(_inputPath);
            if (!records.Any())
                throw new Exception($"{_fileInputFormat.GetType()} did not return any records.");
            Logger.Info("[{0}]:: [{1}] Number of input records loaded.", "INPUT", records.Count());
            return records;
        }
```

# Split
In this step, Hadoop splits / divides our sample input file into different parts, each part made up of some line from the input file. Note that, for the purpose of this course work, we are considering one cpu core as a one split. However, this is not necessarily true in a real-time scenario.
By implementing an iterator pattern over input records, I splitted all the input records to the different MapContext and add each chunk of input records to one MapContext.

```c#
public IEnumerable<MapContext<TKEYIN, TVALUEIN>> RunSplittPhase(IEnumerable<string> inputs)
        {
            Logger.Info("[{0}]:: splitting phase is starting...", "SPLIT");
            var mappersContext = new List<MapContext<TKEYIN, TVALUEIN>>();
            var split = new InputSplit(inputs);
            foreach (var chunk in split)
            {
                Logger.Debug("          create a new map context for [{0}] number of input records.", chunk.Count());
                mappersContext.Add(new MapContext<TKEYIN, TVALUEIN>(chunk));
            }
            Logger.Info("[{0}]:: splitting phase is completed with [{1}] number of map contexts.", "SPLIT", mappersContext.Count());
            return mappersContext;
        }
```

# Map
Map Task in MapReduce is performed using the Map() function. This part of the MapReduce is responsible for processing one or more chunks of data and producing the output results.
Here I created a new MapTask for each MapContext and processed each split according to the logic defined in the Mapper class. Each mapper works on each split at a time. Each mapper is treated as a task and multiple tasks are executed across different Tasks and coordinated by the TaskManager.

```c#
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
```

# Shuffle and Sort
In this step, outputs from all the mappers are shuffled, sorted to put them in order, and grouped before sending them to the next step. I created a new shuffling context for each CPU core which consists of a different number of Reduce Context. So, in the next phase, when we want to rescue the result of mapping we have an equal number of reduced concurrent tasks to the number of available cores on the system under the run.

```c#
public IEnumerable<ShufflingContext<TKEYIN, TVALUEIN>> RunShuffelPhase(Type mapperType, IEnumerable<MapContext<TKEYIN, TVALUEIN>> mapperContexts)
        {
            Logger.Info("[{0}]:: shuffling phase is starting...", "SHFFL");
            var keysValues = SortKeys(CombineKeysValues(mapperContexts));
            Logger.Debug("          [{0}] number of keys and [{1}] number of values are combined ans sorted for shuffling.", keysValues.Count(), keysValues.Values.SelectMany(x => x).Count());

            var contexts = InitNeededContext(Environment.ProcessorCount);
            Logger.Debug("          create [{0}] number of shuffling context.", contexts.Count());
            var numberOfKeysPerContext = Convert.ToInt32(Math.Ceiling(keysValues.Count / (decimal)Environment.ProcessorCount));
            var pageIndex = 0;
            foreach (var context in contexts)
            {
                var contextKeysValue = keysValues.Skip(pageIndex * numberOfKeysPerContext).Take(numberOfKeysPerContext).ToList();
                Logger.Debug("          add [{0}] number of keys into #[{1}] context.", contextKeysValue.Count(), pageIndex + 1);
                foreach (var keyValues in contextKeysValue)
                {
                    context.AddKeyValue(keyValues.Key, keyValues.Value);
                }
                pageIndex++;
            }
            Logger.Info("[{0}]:: shuffling phase is completed.", "SHFFL");
            return contexts;
        }
```

# Reduce
This step is used to aggregate the outputs of mappers using the reduce() function. Output of the reducer is sent to the next and final step. Each reducer is treated as a task and multiple tasks are executed across different Tasks and coordinated by the TaskManager. Just like Mapper, each task processed each split according to the logic defined in the Reducer class.

```c#
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
```
