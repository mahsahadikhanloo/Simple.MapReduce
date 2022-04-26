using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simple.MapReduce.Core.Format.CSV
{
    public class CsvOutpurFormat : FileOutputFormat
    {
        public override bool Write<TKEYOUT, TVALUEOUT>(string filePath, IEnumerable<(TKEYOUT Key, TVALUEOUT Value)> output)
        {
            var lines = output.Select(x => $"{x.Key},{x.Value}").ToList();
            lines.Insert(0, "Key,Value");
            File.WriteAllLines(filePath, lines);
            return true;
        }
    }
}
