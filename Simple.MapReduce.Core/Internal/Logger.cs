using System.Drawing;
using Console = Colorful.Console;

namespace Simple.MapReduce.Core.Internal
{
    internal class Logger
    {
        private static void Write(string template, Color mainColor, Color formatColor, params object[] values)
        {
            Console.WriteLineFormatted(template, mainColor, formatColor, values);
        }

        public static void Info(string template, params object[] values) => Write(template, Color.Green, Color.White, values);
        public static void Debug(string template, params object[] values) => Write(template, Color.Orange, Color.Yellow, values);
        public static void Error(string template, params object[] values) => Write(template, Color.Red, Color.Red, values);
    }
}
