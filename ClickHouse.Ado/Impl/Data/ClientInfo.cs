using System;
using System.Net;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Impl.Data {
    internal class ClientInfo {
        public ClientInfo() {
            QueryKind = QueryKind.Initial;
            Interface = Interface.Tcp;
            HttpMethod = HttpMethod.Unknown;
            ClientName = ProtocolCaps.ClientName;
            ClientVersionMajor = 1;
            ClientVersionMinor = 1;
            ClientRevision = 54411;
        }

        public QueryKind QueryKind { get; set; }

        public string CurrentUser { get; set; }
        public string CurrentQueryId { get; set; }
        public EndPoint CurrentAddress { get; set; }
        public string InitialUser { get; set; }
        public string InitialQueryId { get; set; }
        public EndPoint InitialAddress { get; set; }
        public Interface Interface { get; set; }
        public string OsUser { get; set; }
        public string ClientHostname { get; set; }
        public string ClientName { get; set; }
        public long ClientVersionMajor { get; }
        public long ClientVersionMinor { get; }
        public uint ClientRevision { get; }
        public HttpMethod HttpMethod { get; set; }
        public string HttpUserAgent { get; set; }
        public string QuotaKey { get; set; }

        internal void Write(ProtocolFormatter formatter)
        {
            WriteAsync(formatter).Wait();
        }

        internal async Task WriteAsync(ProtocolFormatter formatter)
        {
            await formatter.WriteByteAsync((byte)QueryKind).ConfigureAwait(false);
            if (QueryKind == QueryKind.None) return;
            await formatter.WriteStringAsync(InitialUser).ConfigureAwait(false);
            await formatter.WriteStringAsync(InitialQueryId).ConfigureAwait(false);
            await formatter.WriteStringAsync(InitialAddress?.ToString()).ConfigureAwait(false);
            await formatter.WriteByteAsync((byte)Interface).ConfigureAwait(false);
            switch (Interface)
            {
                case Interface.Tcp:
                    await formatter.WriteStringAsync(OsUser).ConfigureAwait(false);
                    await formatter.WriteStringAsync(ClientHostname).ConfigureAwait(false);
                    await formatter.WriteStringAsync(ClientName).ConfigureAwait(false);
                    await formatter.WriteUIntAsync(ClientVersionMajor).ConfigureAwait(false);
                    await formatter.WriteUIntAsync(ClientVersionMinor).ConfigureAwait(false);
                    await formatter.WriteUIntAsync(ClientRevision).ConfigureAwait(false);
                    break;
                case Interface.Http:
                    await formatter.WriteByteAsync((byte)HttpMethod).ConfigureAwait(false);
                    await formatter.WriteStringAsync(HttpUserAgent).ConfigureAwait(false);
                    break;
            }

            if (formatter.ServerInfo.Build > ProtocolCaps.DbmsMinRevisionWithQuotaKeyInClientInfo)
                await formatter.WriteStringAsync(QuotaKey).ConfigureAwait(false);
            if (formatter.ServerInfo.Build > ProtocolCaps.DbmsMinRevisionWithServerVersionPatch)
                await formatter.WriteUIntAsync(ClientRevision).ConfigureAwait(false);
        }

        public void PopulateEnvironment() {
#if !NETSTANDARD15 && !NETCOREAPP11
			OsUser = Environment.UserName;
#endif
            ClientHostname = Environment.MachineName;
        }
    }
}