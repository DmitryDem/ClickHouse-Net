using System;
using System.IO;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.Data;
using LZ4;

namespace ClickHouse.Ado.Impl.Compress {
    internal class Lz4Compressor : HashingCompressor {
        private static readonly byte[] Header = {0x82};

        private readonly bool _useHc;

        public Lz4Compressor(bool useHc, ClickHouseConnectionSettings settings) : base(settings) => _useHc = useHc;

        public override CompressionMethod Method => _useHc ? CompressionMethod.Lz4Hc : CompressionMethod.Lz4;

        protected override byte[] Compress(MemoryStream uncompressed)
        {
            return CompressAsync(uncompressed).Result;
        }

        protected override async Task<byte[]> CompressAsync(MemoryStream uncompressed)
        {
            var output = new MemoryStream();
            await output.WriteAsync(Header, 0, Header.Length).ConfigureAwait(false);
            var compressed = _useHc ? LZ4Codec.EncodeHC(uncompressed.ToArray(), 0, (int)uncompressed.Length) : LZ4Codec.Encode(uncompressed.ToArray(), 0, (int)uncompressed.Length);
            await output.WriteAsync(BitConverter.GetBytes(compressed.Length + 9), 0, 4).ConfigureAwait(false);
            await output.WriteAsync(BitConverter.GetBytes(uncompressed.Length), 0, 4).ConfigureAwait(false);
            await output.WriteAsync(compressed, 0, compressed.Length).ConfigureAwait(false);
            return output.ToArray();
        }

        protected override byte[] Decompress(Stream compressed, out UInt128 compressedHash)
        {
            var result = DecompressAsync(compressed).Result;
            compressedHash = result.Item2;
            return result.Item1;
        }

        protected override async Task<Tuple<byte[], UInt128>> DecompressAsync(Stream compressed)
        {
            var header = new byte[9];
            var read = 0;
            do
            {
                read += await compressed.ReadAsync(header, read, header.Length - read).ConfigureAwait(false);
            } while (read < header.Length);

            if (header[0] != Header[0])
                throw new FormatException($"Invalid header value {header[0]}");

            var compressedSize = BitConverter.ToInt32(header, 1);
            var uncompressedSize = BitConverter.ToInt32(header, 5);
            read = 0;
            compressedSize -= header.Length;
            var compressedBytes = new byte[compressedSize + header.Length];
            Array.Copy(header, 0, compressedBytes, 0, header.Length);
            do
            {
                read += await compressed.ReadAsync(compressedBytes, header.Length + read, compressedSize - read).ConfigureAwait(false);
            } while (read < compressedSize);

            var compressedHash = ClickHouseCityHash.CityHash128(compressedBytes);
            var decompressedBytes = LZ4Codec.Decode(compressedBytes, header.Length, compressedSize, uncompressedSize);
            return new Tuple<byte[], UInt128>(decompressedBytes, compressedHash);
        }
    }
}