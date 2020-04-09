using System;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Settings {
    internal class UInt64SettingValue : SettingValue {
        public UInt64SettingValue(ulong value) => Value = value;

        public ulong Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter) => WriteAsync(formatter).Wait();

        protected internal override async Task WriteAsync(ProtocolFormatter formatter) => await formatter.WriteUIntAsync((long)Value).ConfigureAwait(false);

        internal override T As<T>() {
            if (typeof(T) != typeof(ulong)) throw new InvalidCastException();
            return (T) (object) Value;
        }

        internal override object AsValue() => Value;
    }
}