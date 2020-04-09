using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
#if !NETCOREAPP11
#endif

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class NullableColumnType : ColumnType {
        public NullableColumnType(ColumnType innerType) => InnerType = innerType;

        public override bool IsNullable => true;
        public override int Rows => InnerType.Rows;
        internal override Type CLRType => InnerType.CLRType.IsByRef ? InnerType.CLRType : typeof(Nullable<>).MakeGenericType(InnerType.CLRType);

        public ColumnType InnerType { get; }
        public bool[] Nulls { get; private set; }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => $"Nullable({InnerType.AsClickHouseType(usageIntent)})";

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            WriteAsync(formatter, rows).Wait();
        }

        public override async Task WriteAsync(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            await new SimpleColumnType<byte>(Nulls.Select(x => x ? (byte)1 : (byte)0).ToArray()).WriteAsync(formatter, rows).ConfigureAwait(false);
            await InnerType.WriteAsync(formatter, rows).ConfigureAwait(false);
        }

        internal override void Read(ProtocolFormatter formatter, int rows) {
            var nullStatuses = new SimpleColumnType<byte>();
            nullStatuses.Read(formatter, rows);
            Nulls = nullStatuses.Data.Select(x => x != 0).ToArray();
            InnerType.Read(formatter, rows);
        }

        internal override async Task ReadAsync(ProtocolFormatter formatter, int rows)
        {
            var nullStatuses = new SimpleColumnType<byte>();
            await nullStatuses.ReadAsync(formatter, rows).ConfigureAwait(false);
            Nulls = nullStatuses.Data.Select(x => x != 0).ToArray();
            await InnerType.ReadAsync(formatter, rows).ConfigureAwait(false);
        }

        public override void ValueFromConst(Parser.ValueType val) {
            Nulls = new[] {val.StringValue == null && val.ArrayValue == null};
            InnerType.ValueFromConst(val);
        }

        public override void ValueFromParam(ClickHouseParameter parameter) {
            Nulls = new[] {parameter.Value == null};
            InnerType.ValueFromParam(parameter);
        }

        public override object Value(int currentRow) => Nulls[currentRow] ? null : InnerType.Value(currentRow);

        public override long IntValue(int currentRow) {
            if (Nulls[currentRow])
#if NETSTANDARD15 || NETCOREAPP11
                throw new ArgumentNullException();
#else
				throw new System.Data.SqlTypes.SqlNullValueException();
#endif
            return InnerType.IntValue(currentRow);
        }

        public override void ValuesFromConst(IEnumerable objects) {
            InnerType.NullableValuesFromConst(objects);
            Nulls = objects.Cast<object>().Select(x => x == null).ToArray();
            //Data = objects.Cast<DateTime>().ToArray();
        }

        public bool IsNull(int currentRow) => Nulls[currentRow];
    }
}