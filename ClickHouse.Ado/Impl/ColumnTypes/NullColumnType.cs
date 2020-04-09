using System;
using System.Collections;
using System.Diagnostics;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado.Impl.ColumnTypes {
    internal class NullColumnType : ColumnType {
        private int _rows;

        public override int Rows => _rows;
        internal override Type CLRType => typeof(object);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            ReadAsync(formatter, rows).Wait();
        }

        internal override async Task ReadAsync(ProtocolFormatter formatter, int rows)
        {
            await new SimpleColumnType<byte>().ReadAsync(formatter, rows).ConfigureAwait(false);
            _rows = rows;
        }

        public override void ValueFromConst(Parser.ValueType val) { }

        public override string AsClickHouseType(ClickHouseTypeUsageIntent usageIntent) => "Null";

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            WriteAsync(formatter, rows).Wait();
        }

        public override async Task WriteAsync(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            await new SimpleColumnType<byte>(new byte[rows]).ReadAsync(formatter, rows).ConfigureAwait(false);
        }

        public override void ValueFromParam(ClickHouseParameter parameter) { }

        public override object Value(int currentRow) => null;

        public override long IntValue(int currentRow) => 0;

        public override void ValuesFromConst(IEnumerable objects) { }

        public override void NullableValuesFromConst(IEnumerable objects) { }
    }
}