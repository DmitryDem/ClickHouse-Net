using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.Compress;
using ClickHouse.Ado.Impl.Data;
using ClickHouse.Ado.Impl.Settings;

namespace ClickHouse.Ado.Impl
{
    internal class ProtocolFormatter
    {
        private static readonly Regex NameRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*$", RegexOptions.Compiled);

        /// <summary>
        ///     Underlaying stream, usually NetworkStream.
        /// </summary>
        private readonly Stream _baseStream;

        private readonly Func<bool> _poll;
        private readonly int _socketTimeout;

        private Compressor _compressor;

        /// <summary>
        ///     Compressed stream, !=null indicated that compression/decompression has beed started.
        /// </summary>
        private Stream _compStream;

        private ClickHouseConnectionSettings _connectionSettings;

        /// <summary>
        ///     Stream to write to/read from, either _baseStream or _compStream.
        /// </summary>
        private Stream _ioStream;

        internal ProtocolFormatter(Stream baseStream, ClientInfo clientInfo, Func<bool> poll, int socketTimeout)
        {
            _baseStream = baseStream;
            _poll = poll;
            _socketTimeout = socketTimeout;
            _ioStream = _baseStream;
            /*reader = new BinaryReader(_baseStream,Encoding.UTF8);
            writer = new BinaryWriter(_baseStream);*/
            ClientInfo = clientInfo;
        }

        public ServerInfo ServerInfo { get; set; }
        public ClientInfo ClientInfo { get; }

        public void Handshake(ClickHouseConnectionSettings connectionSettings)
        {
            HandshakeAsync(connectionSettings).Wait();
        }

        public async Task HandshakeAsync(ClickHouseConnectionSettings connectionSettings)
        {
            _connectionSettings = connectionSettings;
            _compressor = connectionSettings.Compress ? Compressor.Create(connectionSettings) : null;
            await WriteUIntAsync((int)ClientMessageType.Hello).ConfigureAwait(false);

            await WriteStringAsync(ClientInfo.ClientName).ConfigureAwait(false);
            await WriteUIntAsync(ClientInfo.ClientVersionMajor).ConfigureAwait(false);
            await WriteUIntAsync(ClientInfo.ClientVersionMinor).ConfigureAwait(false);
            await WriteUIntAsync(ClientInfo.ClientRevision).ConfigureAwait(false);
            await WriteStringAsync(connectionSettings.Database).ConfigureAwait(false);
            await WriteStringAsync(connectionSettings.User).ConfigureAwait(false);
            await WriteStringAsync(connectionSettings.Password).ConfigureAwait(false);
            await _ioStream.FlushAsync().ConfigureAwait(false);

            var serverHello = await ReadUIntAsync().ConfigureAwait(false);
            if (serverHello == (int)ServerMessageType.Hello)
            {
                var serverName = await ReadStringAsync().ConfigureAwait(false);
                var serverMajor = await ReadUIntAsync().ConfigureAwait(false);
                var serverMinor = await ReadUIntAsync().ConfigureAwait(false);
                var serverBuild = await ReadUIntAsync().ConfigureAwait(false);
                string serverTz = null, serverDn = null;
                ulong serverPatch = 0;
                if (serverBuild >= ProtocolCaps.DbmsMinRevisionWithServerTimezone)
                    serverTz = await ReadStringAsync().ConfigureAwait(false);
                if (serverBuild >= ProtocolCaps.DbmsMinRevisionWithServerDisplayName)
                    serverDn = await ReadStringAsync().ConfigureAwait(false);
                if (serverBuild >= ProtocolCaps.DbmsMinRevisionWithServerVersionPatch)
                    serverPatch = (uint)(await ReadUIntAsync().ConfigureAwait(false));
                ServerInfo = new ServerInfo
                {
                    Build = serverBuild,
                    Major = serverMajor,
                    Minor = serverMinor,
                    Name = serverName,
                    Timezone = serverTz,
                    Patch = (long)serverPatch,
                    DisplayName = serverDn
                };
            }
            else if (serverHello == (int)ServerMessageType.Exception)
            {
                await ReadAndThrowExceptionAsync().ConfigureAwait(false);
            }
            else
            {
                throw new FormatException($"Bad message type {serverHello:X} received from server.");
            }
        }

        internal void RunQuery(string sql,
                               QueryProcessingStage stage,
                               QuerySettings settings,
                               ClientInfo clientInfo,
                               IEnumerable<Block> xtables,
                               bool noData)
        {
            RunQueryAsync(sql, stage, settings, clientInfo, xtables, noData).Wait();
        }

        internal async Task RunQueryAsync(string sql,
            QueryProcessingStage stage,
            QuerySettings settings,
            ClientInfo clientInfo,
            IEnumerable<Block> xtables,
            bool noData)
        {
            await WriteUIntAsync((int)ClientMessageType.Query).ConfigureAwait(false);
            await WriteStringAsync("").ConfigureAwait(false);
            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithClientInfo)
            {
                if (clientInfo == null)
                    clientInfo = ClientInfo;
                else
                    clientInfo.QueryKind = QueryKind.Secondary;

                await clientInfo.WriteAsync(this).ConfigureAwait(false);
            }

            var compressionMethod = _compressor != null ? _compressor.Method : CompressionMethod.Lz4;
            if (settings != null)
            {
                await settings.WriteAsync(this).ConfigureAwait(false);
                compressionMethod = settings.Get<CompressionMethod>("compression_method");
            }
            else
            {
                await WriteStringAsync("").ConfigureAwait(false);
            }

            await WriteUIntAsync((int)stage).ConfigureAwait(false);
            await WriteUIntAsync(_connectionSettings.Compress ? (int)compressionMethod : 0).ConfigureAwait(false);
            await WriteStringAsync(sql).ConfigureAwait(false);
            await _baseStream.FlushAsync().ConfigureAwait(false);

            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables && noData)
            {
                await new Block().WriteAsync(this).ConfigureAwait(false);
                await _baseStream.FlushAsync().ConfigureAwait(false);
            }

            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTemporaryTables)
            {
                await SendBlocksAsync(xtables).ConfigureAwait(false);
            }
        }

        internal Block ReadSchema()
        {
            return ReadSchemaAsync().Result;
        }

        internal async Task<Block> ReadSchemaAsync()
        {
            var schema = new Response();
            if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithColumnDefaultsMetadata)
            {
                await ReadPacketAsync(schema).ConfigureAwait(false);
                if (schema.Type == ServerMessageType.TableColumns)
                    await ReadPacketAsync(schema).ConfigureAwait(false);
            }
            else
            {
                await ReadPacketAsync(schema).ConfigureAwait(false);
            }

            return schema.Blocks.First();
        }

        internal void SendBlocks(IEnumerable<Block> blocks)
        {
            SendBlocksAsync(blocks).Wait();
        }

        internal async Task SendBlocksAsync(IEnumerable<Block> blocks)
        {
            if (blocks != null)
                foreach (var block in blocks)
                {
                    await block.WriteAsync(this).ConfigureAwait(false);
                    await _baseStream.FlushAsync().ConfigureAwait(false);
                }

            await new Block().WriteAsync(this).ConfigureAwait(false);
            await _baseStream.FlushAsync().ConfigureAwait(false);
        }

        internal Response ReadResponse()
        {
            return ReadResponseAsync().Result;
        }

        internal async Task<Response> ReadResponseAsync()
        {
            var rv = new Response();
            while (true)
            {
                if (!_poll()) continue;
                if (!await ReadPacketAsync(rv).ConfigureAwait(false)) break;
            }

            return rv;
        }

        internal Block ReadBlock()
        {
            return ReadBlockAsync().Result;
        }

        internal async Task<Block> ReadBlockAsync()
        {
            var rv = new Response();
            while (await ReadPacketAsync(rv).ConfigureAwait(false))
                if (rv.Blocks.Any())
                    return rv.Blocks.First();
            return null;
        }

        internal bool ReadPacket(Response rv)
        {
            return ReadPacketAsync(rv).Result;
        }

        internal async Task<bool> ReadPacketAsync(Response rv)
        {
            var type = (ServerMessageType)await ReadUIntAsync().ConfigureAwait(false);
            rv.Type = type;
            switch (type)
            {
                case ServerMessageType.Data:
                case ServerMessageType.Totals:
                case ServerMessageType.Extremes:
                    rv.AddBlock(await Block.ReadAsync(this).ConfigureAwait(false));
                    return true;
                case ServerMessageType.Exception:
                    await ReadAndThrowExceptionAsync().ConfigureAwait(false);
                    return false;
                case ServerMessageType.Progress:
                    {
                        var rows = await ReadUIntAsync().ConfigureAwait(false);
                        var bytes = await ReadUIntAsync().ConfigureAwait(false);
                        long total = 0;
                        if (ServerInfo.Build >= ProtocolCaps.DbmsMinRevisionWithTotalRowsInProgress)
                            total = await ReadUIntAsync().ConfigureAwait(false);
                        rv.OnProgress(rows, total, bytes);
                        return true;
                    }
                case ServerMessageType.ProfileInfo:
                    {
                        var rows = await ReadUIntAsync().ConfigureAwait(false);
                        var blocks = await ReadUIntAsync().ConfigureAwait(false);
                        var bytes = await ReadUIntAsync().ConfigureAwait(false);
                        var appliedLimit = await ReadUIntAsync().ConfigureAwait(false); //bool
                        var rowsNoLimit = await ReadUIntAsync().ConfigureAwait(false);
                        var calcRowsNoLimit = await ReadUIntAsync().ConfigureAwait(false); //bool
                        return true;
                    }
                case ServerMessageType.TableColumns:
                    {
                        var empty = await ReadStringAsync().ConfigureAwait(false);
                        var columns = await ReadStringAsync().ConfigureAwait(false);
                    }
                    return true;
                case ServerMessageType.Pong:
                    return true;
                case ServerMessageType.EndOfStream:
                    rv.OnEnd();
                    return false;
                default:
                    throw new InvalidOperationException($"Received unknown packet type {type} from server.");
            }
        }

        public static string EscapeName(string str)
        {
            if (!NameRegex.IsMatch(str)) throw new ArgumentException($"'{str}' is invalid identifier.");
            return str;
        }

        public static string EscapeStringValue(string str) => "\'" + str.Replace("\\", "\\\\").Replace("\'", "\\\'") + "\'";

        public void Close()
        {
            if (_compStream != null)
                _compStream.Dispose();
        }

        public static string UnescapeStringValue(string src)
        {
            if (src == null) return string.Empty;
            if (src.StartsWith("'") && src.EndsWith("'")) return src.Substring(1, src.Length - 2).Replace("\\'", "'").Replace("\\\\", "\\");
            return src;
        }

        #region Structures IO

        private void ReadAndThrowException()
        {
            try
            {
                ReadAndThrowExceptionAsync().Wait();
            }
            catch (AggregateException ex)
            {
                var exception = ex.InnerExceptions.First();
                throw exception;
            }
        }

        private async Task ReadAndThrowExceptionAsync()
        {
            var exception = await ReadExceptionAsync().ConfigureAwait(false);
            throw exception;
        }

        private Exception ReadException()
        {
            return ReadExceptionAsync().Result;
        }

        private async Task<Exception> ReadExceptionAsync()
        {
            var code = BitConverter.ToInt32(await ReadBytesAsync(4).ConfigureAwait(false), 0); // reader.ReadInt32Async();
            var name = await ReadStringAsync().ConfigureAwait(false);
            var message = await ReadStringAsync().ConfigureAwait(false);
            var stackTrace = await ReadStringAsync().ConfigureAwait(false);
            var nested = (await ReadBytesAsync(1).ConfigureAwait(false)).Any(x => x != 0);
            if (nested)
                return new ClickHouseException(message, await ReadExceptionAsync().ConfigureAwait(false))
                {
                    Code = code,
                    Name = name,
                    ServerStackTrace = stackTrace
                };
            return new ClickHouseException(message)
            {
                Code = code,
                Name = name,
                ServerStackTrace = stackTrace
            };
        }

        #endregion

        #region Low-level IO

        internal void WriteByte(byte b) => WriteByteAsync(b).Wait();

        internal async Task WriteByteAsync(byte b) => await _ioStream.WriteAsync(new[] { b }, 0, 1).ConfigureAwait(false);

        internal void WriteUInt(long s)
        {
            WriteUIntAsync(s).Wait();
        }

        internal async Task WriteUIntAsync(long s)
        {
            var x = (ulong)s;
            for (var i = 0; i < 9; i++)
            {
                var b = (byte)((byte)x & 0x7f);
                if (x > 0x7f)
                    b |= 0x80;
                await WriteByteAsync(b).ConfigureAwait(false);
                x >>= 7;
                if (x == 0) return;
            }
        }

        internal long ReadUInt()
        {
            return ReadUIntAsync().Result;
        }

        internal async Task<long> ReadUIntAsync()
        {
            var x = 0;
            for (var i = 0; i < 9; ++i)
            {
                var b = await ReadByteAsync().ConfigureAwait(false);
                x |= (b & 0x7F) << (7 * i);

                if ((b & 0x80) == 0) return x;
            }

            return x;
        }

        internal void WriteString(string s)
        {
            WriteStringAsync(s).Wait();
        }

        internal async Task WriteStringAsync(string s)
        {
            if (s == null) s = "";
            var bytes = Encoding.UTF8.GetBytes(s);
            await WriteUIntAsync((uint)bytes.Length).ConfigureAwait(false);
            await WriteBytesAsync(bytes).ConfigureAwait(false);
        }

        internal string ReadString()
        {
            return ReadStringAsync().Result;
        }

        internal async Task<string> ReadStringAsync()
        {
            var len = await ReadUIntAsync().ConfigureAwait(false);
            if (len > int.MaxValue)
                throw new ArgumentException("Server sent too long string.");
            var rv = len == 0 ? string.Empty : Encoding.UTF8.GetString(await ReadBytesAsync((int)len).ConfigureAwait(false));
            return rv;
        }

        public byte[] ReadBytes(int i)
        {
            return ReadBytesAsync(i).Result;
        }

        public async Task<byte[]> ReadBytesAsync(int i)
        {
            var bytes = new byte[i];
            var read = 0;
            var cur = 0;
            var networkStream = _ioStream as NetworkStream ?? (_ioStream as UnclosableStream)?.BaseStream as NetworkStream;
            long waitTimeStamp = 0;

            do
            {
                cur = await _ioStream.ReadAsync(bytes, read, i - read).ConfigureAwait(false);
                read += cur;

                if (cur == 0)
                {
                    // when we read from non-NetworkStream there's no point in waiting for more data
                    if (networkStream == null)
                        throw new EndOfStreamException();
                    // check for DataAvailable forces an exception if socket is closed
                    if (networkStream.DataAvailable)
                        continue;

                    if (waitTimeStamp == 0)
                        waitTimeStamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();

                    // check for socket timeout if we are expecting data, but somehow server is dead or stopped sending data
                    if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - _socketTimeout > waitTimeStamp)
                        throw new TimeoutException("Socket timeout while waiting for data");

                    await Task.Delay(1).ConfigureAwait(false);
                }
                else
                {
                    waitTimeStamp = 0;
                }
            } while (read < i);

            return bytes;
        }

        public byte ReadByte() => ReadBytesAsync(1).Result[0];

        public async Task<byte> ReadByteAsync()
        {
            var bytes = await ReadBytesAsync(1).ConfigureAwait(false);
            return bytes[0];
        }

        public void WriteBytes(byte[] bytes) => WriteBytesAsync(bytes).Wait();

        public async Task WriteBytesAsync(byte[] bytes) => await _ioStream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);

        public void WriteBytes(byte[] bytes, int offset, int len) => WriteBytesAsync(bytes, offset, len).Wait();

        public async Task WriteBytesAsync(byte[] bytes, int offset, int len) => await _ioStream.WriteAsync(bytes, offset, len).ConfigureAwait(false);

        #endregion

        #region Compression

        internal class CompressionHelper
        {
            private readonly ProtocolFormatter _formatter;

            public CompressionHelper(ProtocolFormatter formatter)
            {
                _formatter = formatter;
            }

            public async Task Run(Func<Task> block)
            {
                await _formatter.StartCompressionAsync().ConfigureAwait(false);
                await block().ConfigureAwait(false);
                await _formatter.EndDecompressionAsync().ConfigureAwait(false);
            }
        }

        internal class DecompressionHelper
        {
            private readonly ProtocolFormatter _formatter;

            public DecompressionHelper(ProtocolFormatter formatter)
            {
                _formatter = formatter;
            }

            public async Task Run(Func<Task> block)
            {
                await _formatter.StartDecompressionAsync().ConfigureAwait(false);
                await block().ConfigureAwait(false);
                await _formatter.EndDecompressionAsync().ConfigureAwait(false);
            }
        }

        private void StartCompression()
        {
            StartCompressionAsync().Wait();
        }

        internal async Task StartCompressionAsync()
        {
            if (_connectionSettings.Compress)
            {
                Debug.Assert(_compStream == null, "Already doing compression/decompression!");
                _compStream = _compressor.BeginCompression(_baseStream);
                _ioStream = _compStream;
            }
        }

        private void EndCompression()
        {
            EndCompressionAsync().Wait();
        }

        private async Task EndCompressionAsync()
        {
            if (_connectionSettings.Compress)
            {
                Debug.Assert(_compStream != null, "Compression has not been started!");
                await _compressor.EndCompressionAsync().ConfigureAwait(false);
                _compStream = null;
                _ioStream = _baseStream;
            }
        }

        private void StartDecompression()
        {
            StartDecompressionAsync().Wait();
        }

        private async Task StartDecompressionAsync()
        {
            if (_connectionSettings.Compress)
            {
                Debug.Assert(_compStream == null, "Already doing compression/decompression!");
                _compStream = await _compressor.BeginDecompressionAsync(_baseStream).ConfigureAwait(false);
                _ioStream = _compStream;
            }
        }

        private void EndDecompression()
        {
            EndDecompressionAsync().Wait();
        }

        private async Task EndDecompressionAsync()
        {
            if (_connectionSettings.Compress)
            {
                Debug.Assert(_compStream != null, "Compression has not been started!");
                await _compressor.EndDecompressionAsync();
                _compStream = null;
                _ioStream = _baseStream;
            }
        }

        internal CompressionHelper Compression => new CompressionHelper(this);
        internal DecompressionHelper Decompression => new DecompressionHelper(this);

        #endregion
    }
}