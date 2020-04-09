using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ColumnTypes;

namespace ClickHouse.Ado.Impl.Data
{
    internal partial class ColumnInfo
    {
        public string Name { get; set; }
        public ColumnType Type { get; set; }

        internal void Write(ProtocolFormatter formatter, int rows)
        {
            WriteAsync(formatter, rows).Wait();
        }

        internal async Task WriteAsync(ProtocolFormatter formatter, int rows)
        {
            await formatter.WriteStringAsync(Name).ConfigureAwait(false);
            await formatter.WriteStringAsync(Type.AsClickHouseType(ClickHouseTypeUsageIntent.ColumnInfo)).ConfigureAwait(false);

            if (rows > 0)
                await Type.WriteAsync(formatter, rows).ConfigureAwait(false);
        }

        public static ColumnInfo Read(ProtocolFormatter formatter, int rows)
        {
            return ReadAsync(formatter, rows).Result;
        }

        public static async Task<ColumnInfo> ReadAsync(ProtocolFormatter formatter, int rows)
        {
            var rv = new ColumnInfo();
            rv.Name = await formatter.ReadStringAsync().ConfigureAwait(false);
            rv.Type = ColumnType.Create(await formatter.ReadStringAsync().ConfigureAwait(false));
            if (rows > 0)
                await rv.Type.ReadAsync(formatter, rows).ConfigureAwait(false);
            return rv;
        }
    }
}