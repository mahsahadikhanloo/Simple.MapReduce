using Simple.MapReduce.Core;

namespace Reading.BigData.Coursework
{
    internal interface CourseWorkFactory<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> where TKEYIN : notnull
    {
        public Job<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> Create(string inputPath, string outputPath, CancellationToken cancellationToken);
    }
}
