using Simple.MapReduce.Core.Internal;

namespace Simple.MapReduce.Core
{
    public abstract class Mapper<TKEYOUT, TVALUEOUT> where TKEYOUT : notnull
    {
        protected void Setup(MapContext<TKEYOUT, TVALUEOUT> context) { }
        protected abstract void Map(string value, MapContext<TKEYOUT, TVALUEOUT> context);
        protected void Cleanup(MapContext<TKEYOUT, TVALUEOUT> context) { }

        public void Run(MapContext<TKEYOUT, TVALUEOUT> context, CancellationToken cancellationToken)
        {
            Setup(context);
            try
            {
                foreach (var record in context.Inputs)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        context.Canceld();
                        return;
                    }
                    Map(record, context);
                }
                context.Done();
            }
            catch (Exception exception)
            {
                context.CatchException(exception);
            }
            finally
            {
                Cleanup(context);
            }
        }
    }
}
