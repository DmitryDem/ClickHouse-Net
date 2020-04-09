using System;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings {
    internal class TimeSpanMsSettingValue : TimeSpanSettingValue {
        public TimeSpanMsSettingValue(int milliseconds) : base(TimeSpan.FromMilliseconds(milliseconds)) { }

        public TimeSpanMsSettingValue(TimeSpan value) : base(value) { }

        protected internal override void Write(ProtocolFormatter formatter) => WriteAsync(formatter).Wait();

        protected internal override Task WriteAsync(ProtocolFormatter formatter) => formatter.WriteUIntAsync((long)Value.TotalMilliseconds);
    }
}