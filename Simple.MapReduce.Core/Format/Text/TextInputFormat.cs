using Simple.MapReduce.Core.Internal;

namespace Simple.MapReduce.Core.Format.Csv
{
    public class TextInputFormat : FileInputFormat
    {
        public override IEnumerable<string> Read(string filePath)
        {
            return File.ReadLines(filePath);
        }
    }
}
