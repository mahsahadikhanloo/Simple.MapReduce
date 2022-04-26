using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simple.MapReduce.Core
{
    public abstract class FileOutputFormat
    {
        public abstract bool Write<TKEYOUT, TVALUEOUT>(string filePath, IEnumerable<(TKEYOUT Key, TVALUEOUT Value)> output);

    }
}
