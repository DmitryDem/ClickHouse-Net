using System;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Data
{
    internal class BlockInfo
    {
        public bool IsOwerflow { get; private set; }
        public int BucketNum { get; private set; } = -1;

        internal void Write(ProtocolFormatter formatter)
        {
            WriteAsync(formatter).Wait();
        }

        internal async Task WriteAsync(ProtocolFormatter formatter)
        {
            await formatter.WriteUIntAsync(1).ConfigureAwait(false);
            await formatter.WriteByteAsync(IsOwerflow ? (byte)1 : (byte)0).ConfigureAwait(false);
            await formatter.WriteUIntAsync(2).ConfigureAwait(false);
            await formatter.WriteBytesAsync(BitConverter.GetBytes(BucketNum)).ConfigureAwait(false);
            await formatter.WriteUIntAsync(0).ConfigureAwait(false);
        }

        public static BlockInfo Read(ProtocolFormatter formatter)
        {
            return ReadAsync(formatter).Result;
        }

        public static async Task<BlockInfo> ReadAsync(ProtocolFormatter formatter)
        {
            long fieldNum;
            var rv = new BlockInfo();

            while ((fieldNum = await formatter.ReadUIntAsync().ConfigureAwait(false)) != 0)
                switch (fieldNum)
                {
                    case 1:
                        var bytes = await formatter.ReadBytesAsync(1).ConfigureAwait(false);
                        rv.IsOwerflow = bytes[0] != 0;
                        break;
                    case 2:
                        rv.BucketNum = BitConverter.ToInt32(await formatter.ReadBytesAsync(4).ConfigureAwait(false), 0);
                        break;
                    default:
                        throw new InvalidOperationException("Unknown field number {0} in block info.");
                }

            return rv;
        }
    }
}