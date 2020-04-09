using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class StringColumnType : ColumnType {
        public StringColumnType() { }

        public StringColumnType(string[] data) => Data = data;

        public string[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;
        internal override Type CLRType => typeof(string);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            ReadAsync(formatter, rows).Wait();
        }

        internal override async Task ReadAsync(ProtocolFormatter formatter, int rows)
        {
            Data = new string[rows];
            for (var i = 0; i < rows; i++) Data[i] = await formatter.ReadStringAsync().ConfigureAwait(false);
        }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => "String";

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            WriteAsync(formatter, rows).Wait();
        }

        public override async Task WriteAsync(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data) await formatter.WriteStringAsync(d).ConfigureAwait(false);
        }

        public override void ValueFromConst(Parser.ValueType val) {
            if (val.TypeHint == Parser.ConstType.String) {
                var uvalue = ProtocolFormatter.UnescapeStringValue(val.StringValue);
                Data = new[] {uvalue};
            } else {
                Data = new[] {val.StringValue};
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter) => Data = new[] {parameter.Value?.ToString()};

        public override object Value(int currentRow) => Data[currentRow];

        public override long IntValue(int currentRow) => throw new InvalidCastException();

        public override void ValuesFromConst(IEnumerable objects) => Data = objects.Cast<string>().ToArray();
    }
}