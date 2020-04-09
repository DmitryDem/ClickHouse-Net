using System;
using System.Globalization;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings {
    internal class FloatSettingValue : SettingValue {
        public FloatSettingValue(float value) => Value = value;

        public float Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => WriteAsync(formatter).Wait();
        protected internal override async Task WriteAsync(ProtocolFormatter formatter) => await formatter.WriteStringAsync(Value.ToString(CultureInfo.InvariantCulture)).ConfigureAwait(false);

        internal override T As<T>() {
            if (typeof(T) != typeof(float)) throw new InvalidCastException();
            return (T) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}