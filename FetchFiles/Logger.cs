using NLog.LayoutRenderers;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FetchFiles
{
    public class Logger : ILogger
    {
        private readonly global::NLog.Logger _logger;

        public Logger(string name)
        {
            LayoutRenderer.Register<ElapsedTimeLayoutRenderer>("elapsed-time");
            _logger = LogManager.GetLogger(name);
        }

        public void Trace(string message)
        {
            _logger.Trace(message);
        }

        public void Trace(string message, Exception exception)
        {
            _logger.Trace(exception, message);
        }

        public void Debug(string message)
        {
            _logger.Debug(message);
        }

        public void Debug(string message, Exception exception)
        {
            _logger.Debug(exception, message);
        }

        public void Info(string message)
        {
            _logger.Info(message);
        }

        public void Info(string message, Exception exception)
        {
            _logger.Info(exception, message);
        }

        public void Warn(string message)
        {
            _logger.Warn(message);
        }

        public void Warn(string message, Exception exception)
        {
            _logger.Warn(exception, message);
        }

        public void Error(string message)
        {
            _logger.Error(message);
        }

        public void Error(string message, Exception exception)
        {
            _logger.Error(exception, message);
        }

        public void Fatal(string message)
        {
            _logger.Fatal(message);
        }

        public void Fatal(string message, Exception exception)
        {
            _logger.Fatal(exception, message);
        }
    }


    [LayoutRenderer("elapsed-time")]
    public class ElapsedTimeLayoutRenderer : LayoutRenderer
    {
        private DateTime? _lastTimeStamp;

        protected override void Append(StringBuilder builder, LogEventInfo logEvent)
        {
            var lastTimeStamp = _lastTimeStamp ?? logEvent.TimeStamp;
            var elapsedTime = logEvent.TimeStamp - lastTimeStamp;
            var elapsedTimeString = $"{elapsedTime.TotalSeconds:f4}".PadLeft(10);
            builder.Append($"{elapsedTimeString}");
            _lastTimeStamp = logEvent.TimeStamp;
        }
    }
}
