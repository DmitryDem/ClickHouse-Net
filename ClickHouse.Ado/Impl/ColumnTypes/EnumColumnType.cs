using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
#if !NETCOREAPP11
using System.Data;
#endif

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class EnumColumnType : ColumnType {
        public EnumColumnType(int baseSize, IEnumerable<Tuple<string, int>> values) {
            Values = values;
            BaseSize = baseSize;
        }

        public IEnumerable<Tuple<string, int>> Values { get; }
        public int BaseSize { get; }
        public int[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;
        internal override Type CLRType => typeof(int);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            ReadAsync(formatter, rows).Wait();
        }

        internal override async Task ReadAsync(ProtocolFormatter formatter, int rows)
        {
            if (BaseSize == 8)
            {
                var vals = new SimpleColumnType<byte>();
                await vals.ReadAsync(formatter, rows).ConfigureAwait(false);
                Data = vals.Data.Select(x => (int)x).ToArray();
            }
            else if (BaseSize == 16)
            {
                var vals = new SimpleColumnType<short>();
                await vals.ReadAsync(formatter, rows).ConfigureAwait(false);
                Data = vals.Data.Select(x => (int)x).ToArray();
            }
            else
            {
                throw new NotSupportedException($"Enums with base size {BaseSize} are not supported.");
            }
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"Enum{BaseSize}({string.Join(",", Values.Select(x => $"{x.Item1}={x.Item2}"))})";

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            WriteAsync(formatter, rows).Wait();
        }

        public override async Task WriteAsync(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            if (BaseSize == 8)
                await new SimpleColumnType<byte>(Data.Select(x => (byte)x).ToArray()).WriteAsync(formatter, rows).ConfigureAwait(false);
            else if (BaseSize == 16)
                await new SimpleColumnType<short>(Data.Select(x => (short)x).ToArray()).WriteAsync(formatter, rows).ConfigureAwait(false);
            else
                throw new NotSupportedException($"Enums with base size {BaseSize} are not supported.");
        }

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String) {
                var uvalue = ProtocolFormatter.UnescapeStringValue(val.StringValue);
                Data = new[] {Values.First(x => x.Item1 == uvalue).Item2};
            } else {
                Data = new[] {int.Parse(val.StringValue)};
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter) {
            if (parameter.DbType == DbType.String
#if !NETCOREAPP11
                || parameter.DbType == DbType.StringFixedLength || parameter.DbType == DbType.AnsiString || parameter.DbType == DbType.AnsiStringFixedLength
#endif
            )
                Data = new[] {Values.First(x => x.Item1 == parameter.Value?.ToString()).Item2};
            else if (
#if NETCOREAPP11
                parameter.DbType==DbType.Integral
#else
                parameter.DbType == DbType.Int16 || parameter.DbType == DbType.Int32 || parameter.DbType == DbType.Int64 || parameter.DbType == DbType.UInt16 || parameter.DbType == DbType.UInt32 ||
                parameter.DbType == DbType.UInt64
#endif
            )
                Data = new[] {(int) Convert.ChangeType(parameter.Value, typeof(int))};
            else throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to Enum.");
        }

        public override object Value(int currentRow) => Data[currentRow];

        public override long IntValue(int currentRow) => Data[currentRow];

        public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<int>().ToArray();

        public override void NullableValuesFromConst(IEnumerable objects) => Data = objects.Cast<int?>().Select(x => x.GetValueOrDefault()).ToArray();
    }
}