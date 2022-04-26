namespace Simple.MapReduce.Core
{
    public abstract class FileInputFormat
    {
        public abstract IEnumerable<string> Read(string filePath);
    }
}
