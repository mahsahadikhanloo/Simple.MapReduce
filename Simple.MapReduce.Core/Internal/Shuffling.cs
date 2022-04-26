using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simple.MapReduce.Core.Internal
{
    internal class Shuffling<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> where TKEYIN : notnull
    {
        private readonly CancellationToken _cancellationToken;
        public Shuffling(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
        }

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

        private Dictionary<TKEYIN, IList<TVALUEIN>> CombineKeysValues(IEnumerable<MapContext<TKEYIN, TVALUEIN>> mapperContexts)
        {
            var keyValues = new Dictionary<TKEYIN, IList<TVALUEIN>>();
            foreach (var context in mapperContexts)
            {
                foreach ((var key, var value) in context.Result)
                {
                    if (!keyValues.ContainsKey(key))
                    {
                        keyValues.Add(key, new List<TVALUEIN>());
                    }
                    keyValues[key].Add(value);
                }
            }
            return keyValues;

        }
        private Dictionary<TKEYIN, IList<TVALUEIN>> SortKeys(Dictionary<TKEYIN, IList<TVALUEIN>> keys)
        {
            return keys.OrderBy(x => x.Key).ToDictionary(x => x.Key, x => x.Value);
        }

        private IEnumerable<ShufflingContext<TKEYIN, TVALUEIN>> InitNeededContext(int numberOfReducer)
        {
            var shuffelingContexts = new ShufflingContext<TKEYIN, TVALUEIN>[numberOfReducer];
            for (int i = 0; i < numberOfReducer; i++)
            {
                shuffelingContexts[i] = new ShufflingContext<TKEYIN, TVALUEIN>();
            }
            return shuffelingContexts;
        }
    }
}
