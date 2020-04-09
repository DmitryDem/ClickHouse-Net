using System.Data;
using System.Threading.Tasks;

namespace ClickHouse.Ado
{
    internal class ClickHouseDataReaderFactory
    {
#if !NETCOREAPP11
        internal async Task<ClickHouseDataReader> CreateReaderAsync(ClickHouseConnection clickHouseConnection, CommandBehavior behavior)
        {
            var reader = new ClickHouseDataReader(clickHouseConnection, behavior);
            await reader.NextResultAsync().ConfigureAwait(false);
            return reader;
        }
#else
        internal async Task<ClickHouseDataReader> CreateReaderAsync(ClickHouseConnection clickHouseConnection)
        {
            var reader = new ClickHouseDataReader(clickHouseConnection);
            await reader.NextResultAsync().ConfigureAwait(false);
            return reader;
        }
#endif
    }
}
