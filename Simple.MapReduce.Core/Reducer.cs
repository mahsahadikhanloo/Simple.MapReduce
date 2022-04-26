using Simple.MapReduce.Core.Internal;

namespace Simple.MapReduce.Core
{
    public abstract class Reducer<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT>
    {
        protected void Setup(ReduceContext<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> context) { }
        protected abstract void Reduce(ReduceContext<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> context);
        protected void Cleanup(ReduceContext<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> context) { }

        public void Run(ReduceContext<TKEYIN, TVALUEIN, TKEYOUT, TVALUEOUT> context, CancellationToken cancellationToken)
        {
            Setup(context);
            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    context.Canceld();
                    return;
                }
                Reduce(context);
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
