using Simple.MapReduce.Core;
using Simple.MapReduce.Core.Format.Csv;
using Simple.MapReduce.Core.Format.CSV;
using Simple.MapReduce.Core.Internal;

namespace Reading.BigData.Coursework
{
    internal class PassengersWithHighestNumberOfFlightsStepOne : CourseWorkFactory<string, int, string, int>
    {
        public Job<string, int, string, int> Create(string inputPath, string outputPath, CancellationToken cancellationToken)
        {
            return new JobBuilder()
                .SetJobName(nameof(PassengersWithHighestNumberOfFlightsStepOne))
                .AddInputPath(inputPath)
                .SetOutputPath(outputPath)
                .SetInputFileFormat<TextInputFormat>()
                .SetOutputFileFormat<CsvOutpurFormat>()
                .SetMapper<PassengersWithHighestNumberOfFlightsStepOneMapper>()
                .SetReducer<PassengersWithHighestNumberOfFlightsStepOneReducer>()
                .Buid<string, int, string, int>(cancellationToken);
        }
    }

    public class PassengersWithHighestNumberOfFlightsStepOneMapper : Mapper<string, int>
    {
        protected override void Map(string value, MapContext<string, int> context)
        {
            if (string.IsNullOrWhiteSpace(value))
                return;

            var cols = value.Split('\u002C');
            if (cols.Length == 6 && !string.IsNullOrEmpty(cols[1]) && !string.IsNullOrEmpty(cols[2]))
                context.Write(cols[0], 1);
        }
    }

    public class PassengersWithHighestNumberOfFlightsStepOneReducer : Reducer<string, int, string, int>
    {
        protected override void Reduce(ReduceContext<string, int, string, int> context)
        {
            context.Write(context.Inputs.Key, context.Inputs.Values.Count());
        }
    }
}
