using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simple.MapReduce.Core.Internal
{
    internal class Splitter<TKEYIN, TVALUEIN> where TKEYIN : notnull
    {
        private readonly int _numberOfSpit = 0;
        public Splitter(int numberOfSplit = 0)
        {
            _numberOfSpit = numberOfSplit;
        }

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
    }
}
