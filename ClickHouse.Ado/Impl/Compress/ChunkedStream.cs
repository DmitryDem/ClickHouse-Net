using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Compress {
    internal class ChunkedStream : Stream {
        private readonly Func<Task<byte[]>> _nextChunkFactory;
        private MemoryStream _currentBlock;

        private ChunkedStream(Func<Task<byte[]>> nextChunkFactory, byte[] nextChunk) {
            _nextChunkFactory = nextChunkFactory;
            _currentBlock = new MemoryStream(nextChunk);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _currentBlock.Length;

        public override long Position { get => _currentBlock.Position; set => throw new NotSupportedException(); }

        public override void Flush() => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer, offset, count).Result;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token)
        {
            var rv = await _currentBlock.ReadAsync(buffer, offset, count, token).ConfigureAwait(false);
            if (rv == 0)
            {
                _currentBlock = new MemoryStream(await _nextChunkFactory().ConfigureAwait(false));
                rv = await _currentBlock.ReadAsync(buffer, offset, count, token).ConfigureAwait(false);
            }

            return rv;
        }

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public static ChunkedStreamFactory Factory { get; } = new ChunkedStreamFactory();

        internal class ChunkedStreamFactory
        {
            internal async Task<ChunkedStream> CreateAsync(Func<Task<byte[]>> nextChunkFactory)
            {
                var nextChunk = await nextChunkFactory().ConfigureAwait(false);
                return new ChunkedStream(nextChunkFactory, nextChunk);
            }
        }
    }
}