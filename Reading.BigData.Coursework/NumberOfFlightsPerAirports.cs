using Simple.MapReduce.Core;
using Simple.MapReduce.Core.Format.Csv;
using Simple.MapReduce.Core.Format.CSV;
using Simple.MapReduce.Core.Internal;

namespace Reading.BigData.Coursework
{
    internal class NumberOfFlightsPerAirports : CourseWorkFactory<string, string, string, int>
    {
        public Job<string, string, string, int> Create(string inputPath, string outputPath, CancellationToken cancellationToken)
        {
            return new JobBuilder()
                .SetJobName(nameof(NumberOfFlightsPerAirports))
                .AddInputPath(inputPath)
                .SetOutputPath(outputPath)
                .SetInputFileFormat<TextInputFormat>()
                .SetOutputFileFormat<CsvOutpurFormat>()
                .SetMapper<NumberOfFlightsPerAirportsMapper>()
                .SetReducer<NumberOfFlightsPerAirportsReducer>()
                .Buid<string, string, string, int>(cancellationToken);
        }
    }

    public class NumberOfFlightsPerAirportsMapper : Mapper<string, string>
    {
        protected override void Map(string value, MapContext<string, string> context)
        {
            if (string.IsNullOrWhiteSpace(value))
                return;

            var cols = value.Split('\u002C');
            if (cols.Length == 6 && !string.IsNullOrEmpty(cols[1]) && !string.IsNullOrEmpty(cols[2]))
                context.Write(cols[2], cols[1]);
        }
    }

    public class NumberOfFlightsPerAirportsReducer : Reducer<string, string, string, int>
    {
        protected override void Reduce(ReduceContext<string, string, string, int> context)
        {
            context.Write(context.Inputs.Key, context.Inputs.Values.Count());
        }
    }
}
