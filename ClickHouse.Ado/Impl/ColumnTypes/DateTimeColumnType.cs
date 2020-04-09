#pragma warning disable CS0618

using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
using Buffer = System.Buffer;
#if !NETCOREAPP11
using System.Data;
#endif

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class DateTimeColumnType : DateColumnType {
        private static readonly DateTime UnixTimeBase = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateTimeColumnType() { }

        public DateTimeColumnType(DateTime[] data) : base(data) { }

        public override int Rows => Data?.Length ?? 0;

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            ReadAsync(formatter, rows).Wait();
        }

        internal override async Task ReadAsync(ProtocolFormatter formatter, int rows)
        {
#if FRAMEWORK20 || FRAMEWORK40 || FRAMEWORK45
            var itemSize = sizeof(uint);
#else
            var itemSize = Marshal.SizeOf<uint>();
#endif
            var bytes = await formatter.ReadBytesAsync(itemSize * rows).ConfigureAwait(false);
            var xdata = new uint[rows];
            Buffer.BlockCopy(bytes, 0, xdata, 0, itemSize * rows);
            Data = xdata.Select(x => UnixTimeBase.AddSeconds(x)).ToArray();
        }

        public override void Write(ProtocolFormatter formatter, int rows) {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
                formatter.WriteBytes(BitConverter.GetBytes((uint) (d - UnixTimeBase).TotalSeconds));
        }

        public override async Task WriteAsync(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
                await formatter.WriteBytesAsync(BitConverter.GetBytes((uint)(d - UnixTimeBase).TotalSeconds)).ConfigureAwait(false);
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => "DateTime";

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String)
                Data = new[] {DateTime.ParseExact(ProtocolFormatter.UnescapeStringValue(val.StringValue), "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.AssumeUniversal)};
            else
                throw new InvalidCastException("Cannot convert numeric value to DateTime.");
        }

        public override void ValueFromParam(ClickHouseParameter parameter) {
            if (parameter.DbType == DbType.Date || parameter.DbType == DbType.DateTime
#if !NETCOREAPP11
                                                || parameter.DbType == DbType.DateTime2 || parameter.DbType == DbType.DateTimeOffset
#endif
            )
                Data = new[] {(DateTime) Convert.ChangeType(parameter.Value, typeof(DateTime))};
            else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to DateTime.");
        }
    }
}