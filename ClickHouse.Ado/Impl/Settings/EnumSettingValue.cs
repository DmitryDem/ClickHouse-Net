using System;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings {
    internal class EnumSettingValue<T> : SettingValue where T : struct {
        public EnumSettingValue(T value) => Value = value;

        public T Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => WriteAsync(formatter).Wait();

        protected internal override async Task WriteAsync(ProtocolFormatter formatter) => await formatter.WriteUIntAsync((long)Convert.ChangeType(Value, typeof(int))).ConfigureAwait(false);

        internal override TX As<TX>() {
            if (typeof(TX) != typeof(T)) throw new InvalidCastException();
            return (TX) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}