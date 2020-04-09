using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Data
{
    internal class Block
    {
        public string Name { get; set; } = "";
        public BlockInfo BlockInfo { get; set; } = new BlockInfo();

        public int Rows => Columns.Count > 0 ? Columns.First().Type.Rows : 0;

        public List<ColumnInfo> Columns { get; } = new List<ColumnInfo>();

        internal void Write(ProtocolFormatter formatter)
        {
            WriteAsync(formatter).Wait();
        }

        internal async Task WriteAsync(ProtocolFormatter formatter)
        {
            await formatter.WriteUIntAsync((int)ClientMessageType.Data).ConfigureAwait(false);
            if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
                await formatter.WriteStringAsync(Name).ConfigureAwait(false);

            await formatter.Compression.Run(async () =>
            {
                if (formatter.ClientInfo.ClientRevision >= ProtocolCaps.DbmsMinRevisionWithBlockInfo)
                {
                    await BlockInfo.WriteAsync(formatter).ConfigureAwait(false);
                }

                await formatter.WriteUIntAsync(Columns.Count).ConfigureAwait(false);
                await formatter.WriteUIntAsync(Rows).ConfigureAwait(false);

                foreach (var column in Columns) await column.WriteAsync(formatter, Rows).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }

        public static Block Read(ProtocolFormatter formatter)
        {
            return ReadAsync(formatter).Result;
        }

        public static async Task<Block> ReadAsync(ProtocolFormatter formatter)
        {
            var rv = new Block();
            if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
                await formatter.ReadStringAsync().ConfigureAwait(false);

            await formatter.Decompression.Run(async () =>
            {
                if (formatter.ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithBlockInfo)
                    rv.BlockInfo = await BlockInfo.ReadAsync(formatter).ConfigureAwait(false);

                var cols = await formatter.ReadUIntAsync().ConfigureAwait(false);
                var rows = await formatter.ReadUIntAsync().ConfigureAwait(false);
                for (var i = 0; i < cols; i++) rv.Columns.Add(await ColumnInfo.ReadAsync(formatter, (int)rows).ConfigureAwait(false));
            }).ConfigureAwait(false);

            return rv;
        }
    }
}