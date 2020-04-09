using System;
using System.IO;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Compress
{
    internal abstract class HashingCompressor : Compressor
    {
        private readonly ClickHouseConnectionSettings _settings;
        private Stream _baseStream;
        private MemoryStream _uncompressed;

        protected HashingCompressor(ClickHouseConnectionSettings settings) => _settings = settings;

        public override Stream BeginCompression(Stream baseStream)
        {
            _baseStream = baseStream;
            return _uncompressed = new MemoryStream();
        }

        public override void EndCompression()
        {
            EndCompressionAsync().Wait();
        }

        public override async Task EndCompressionAsync()
        {
            var compressed = await CompressAsync(_uncompressed).ConfigureAwait(false);
            var hash = ClickHouseCityHash.CityHash128(compressed);
            await _baseStream.WriteAsync(BitConverter.GetBytes(hash.Low), 0, 8).ConfigureAwait(false);
            await _baseStream.WriteAsync(BitConverter.GetBytes(hash.High), 0, 8).ConfigureAwait(false);
            await _baseStream.WriteAsync(compressed, 0, compressed.Length).ConfigureAwait(false);
        }

        public override Stream BeginDecompression(Stream baseStream) => BeginDecompressionAsync(baseStream).Result;

        public override async Task<Stream> BeginDecompressionAsync(Stream baseStream) => await ChunkedStream.Factory.CreateAsync(async () =>
        {
            _baseStream = baseStream;
            var hashRead = new byte[16];
            var read = 0;
            do
            {
                read += await baseStream.ReadAsync(hashRead, read, 16 - read).ConfigureAwait(false);
            } while (read < 16);

            var result = await DecompressAsync(baseStream).ConfigureAwait(false);
            var bytes = result.Item1;
            var hash = result.Item2;

            if (_settings.CheckCompressedHash && BitConverter.ToUInt64(hashRead, 0) != hash.Low)
                throw new ClickHouseException("Checksum verification failed.");
            if (_settings.CheckCompressedHash && BitConverter.ToUInt64(hashRead, 8) != hash.High)
                throw new ClickHouseException("Checksum verification failed.");

            return bytes;
        });

        public override void EndDecompression() { }
        public override async Task EndDecompressionAsync() { }

        protected abstract byte[] Compress(MemoryStream uncompressed);
        protected abstract Task<byte[]> CompressAsync(MemoryStream uncompressed);
        protected abstract byte[] Decompress(Stream compressed, out UInt128 compressedHash);
        protected abstract Task<Tuple<byte[], UInt128>> DecompressAsync(Stream compressed);
    }
}