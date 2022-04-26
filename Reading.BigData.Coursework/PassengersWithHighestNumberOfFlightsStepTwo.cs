using Simple.MapReduce.Core;
using Simple.MapReduce.Core.Format.Csv;
using Simple.MapReduce.Core.Format.CSV;
using Simple.MapReduce.Core.Internal;

namespace Reading.BigData.Coursework
{
    internal class PassengersWithHighestNumberOfFlightsStepTwo : CourseWorkFactory<string, (string, int), string, int>
    {
        public Job<string, (string, int), string, int> Create(string inputPath, string outputPath, CancellationToken cancellationToken)
        {
            return new JobBuilder()
                .SetJobName(nameof(PassengersWithHighestNumberOfFlightsStepTwo))
                .AddInputPath(inputPath)
                .SetOutputPath(outputPath)
                .SetInputFileFormat<TextInputFormat>()
                .SetOutputFileFormat<CsvOutpurFormat>()
                .SetMapper<PassengersWithHighestNumberOfFlightsStepTwoMapper>()
                .SetReducer<PassengersWithHighestNumberOfFlightsStepTwoReducer>()
                .Buid<string, (string, int), string, int>(cancellationToken);
        }
    }

    public class PassengersWithHighestNumberOfFlightsStepTwoMapper : Mapper<string, (string, int)>
    {
        protected override void Map(string value, MapContext<string, (string, int)> context)
        {
            if (string.IsNullOrWhiteSpace(value))
                return;

            var cols = value.Split('\u002C');
            if (cols.Length == 2 && !string.IsNullOrEmpty(cols[0]) && int.TryParse(cols[1], out var number))
                context.Write("All", (cols[0], number));
        }
    }

    public class PassengersWithHighestNumberOfFlightsStepTwoReducer : Reducer<string, (string passenger, int numberOfFlights), string, int>
    {
        protected override void Reduce(ReduceContext<string, (string passenger, int numberOfFlights), string, int> context)
        {
            int maxNumberOfFlight = 0;
            string passengerWithMax = string.Empty;
            foreach (var item in context.Inputs.Values)
            {
                if (item.numberOfFlights > maxNumberOfFlight)
                {
                    maxNumberOfFlight = item.numberOfFlights;
                    passengerWithMax = item.passenger;
                }
            }
            context.Write(passengerWithMax, maxNumberOfFlight);
        }
    }
}
