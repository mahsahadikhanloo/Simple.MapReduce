using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simple.MapReduce.Core.Internal
{
    internal class InputSplit : IEnumerable<IEnumerable<string>>
    {
        private int _index;
        private readonly InputSplitEnum enumerator;
        public InputSplit(IEnumerable<string> records, int numnberOfSplit = 0)
        {

            enumerator = new InputSplitEnum(records);
        }

        public IEnumerator<IEnumerable<string>> GetEnumerator()
        {
            return enumerator;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return enumerator;
        }
    }

    public class InputSplitEnum : IEnumerator<IEnumerable<string>>
    {
        private readonly IEnumerable<string> _records;
        private int _index;
        private int _splitSize;
        public InputSplitEnum(IEnumerable<string> records)
        {
            var numnberOfSplit = Environment.ProcessorCount;
            _records = records;
            _splitSize = records.Count() / numnberOfSplit;
            Reset();
        }

        public IEnumerable<string> Current => _records.Skip(_index * _splitSize).Take(_splitSize);

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            Reset();
        }

        public bool MoveNext()
        {
            _index++;
            if (_index * _splitSize > _records.Count())
                return false;
            return true;
        }

        public void Reset()
        {
            _index = -1;
        }
    }

}
