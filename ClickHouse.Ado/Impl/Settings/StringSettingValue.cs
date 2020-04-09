using System;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings {
    internal class StringSettingValue : SettingValue {
        public StringSettingValue(string value) => Value = value;

        public string Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => WriteAsync(formatter).Wait();

        protected internal override async Task WriteAsync(ProtocolFormatter formatter) => await formatter.WriteStringAsync(Value).ConfigureAwait(false);

        internal override T As<T>() {
            if (typeof(T) != typeof(string)) throw new InvalidCastException();
            return (T) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}