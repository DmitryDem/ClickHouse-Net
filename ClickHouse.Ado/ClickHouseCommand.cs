using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ClickHouse.Ado.Impl.ATG.Insert;
using ClickHouse.Ado.Impl.Data;
#if !NETCOREAPP11
using System.Data;
#endif

namespace ClickHouse.Ado
{
    public class ClickHouseCommand
#if !NETCOREAPP11
        : IDbCommand
#endif
    {
        public ClickHouseCommand() { }

        public ClickHouseCommand(ClickHouseConnection clickHouseConnection) => Connection = clickHouseConnection;

        public ClickHouseCommand(ClickHouseConnection clickHouseConnection, string text) : this(clickHouseConnection) => CommandText = text;

        public void Dispose() { }

        public void Prepare() => throw new NotSupportedException();

        public void Cancel() => throw new NotSupportedException();
        public ClickHouseParameter CreateParameter() => new ClickHouseParameter();
#if !NETCOREAPP11

        IDbDataParameter IDbCommand.CreateParameter() => CreateParameter();

        IDbConnection IDbCommand.Connection { get => Connection; set => Connection = (ClickHouseConnection)value; }
        public IDbTransaction Transaction { get; set; }
        public CommandType CommandType { get; set; }
        IDataParameterCollection IDbCommand.Parameters => Parameters;
        public UpdateRowSource UpdatedRowSource { get; set; }

#endif

        private void Execute(bool readResponse, ClickHouseConnection connection)
        {
            ExecuteAsync(readResponse, connection).Wait();
        }

        private async Task ExecuteAsync(bool readResponse, ClickHouseConnection connection)
        {
            if (connection.State != ConnectionState.Open) throw new InvalidOperationException("Connection isn't open");

            var insertParser = new Parser(new Scanner(new MemoryStream(Encoding.UTF8.GetBytes(CommandText))));
            insertParser.errors.errorStream = new StringWriter();
            insertParser.Parse();

            if (insertParser.errors.count == 0)
            {
                var xText = new StringBuilder("INSERT INTO ");
                xText.Append(insertParser.tableName);
                if (insertParser.fieldList != null)
                {
                    xText.Append("(");
                    insertParser.fieldList.Aggregate(xText, (builder, fld) => builder.Append(fld).Append(','));
                    xText.Remove(xText.Length - 1, 1);
                    xText.Append(")");
                }

                xText.Append(" VALUES");

                await connection.Formatter.RunQueryAsync(xText.ToString(), QueryProcessingStage.Complete, null, null, null, false).ConfigureAwait(false);
                var schema = await connection.Formatter.ReadSchemaAsync().ConfigureAwait(false);
                if (insertParser.oneParam != null)
                {
                    if (Parameters[insertParser.oneParam].Value is IBulkInsertEnumerable bulkInsertEnumerable)
                    {
                        var index = 0;
                        foreach (var col in schema.Columns)
                            col.Type.ValuesFromConst(bulkInsertEnumerable.GetColumnData(index++, col.Name, col.Type.AsClickHouseType(ClickHouseTypeUsageIntent.Generic)));
                    }
                    else
                    {
                        var table = ((IEnumerable)Parameters[insertParser.oneParam].Value).OfType<IEnumerable>();
                        var colCount = table.First().Cast<object>().Count();
                        if (colCount != schema.Columns.Count)
                            throw new FormatException($"Column count in parameter table ({colCount}) doesn't match column count in schema ({schema.Columns.Count}).");
                        var cl = new List<List<object>>(colCount);
                        for (var i = 0; i < colCount; i++)
                            cl.Add(new List<object>());
                        var index = 0;
                        cl = table.Aggregate(
                            cl,
                            (colList, row) =>
                            {
                                index = 0;
                                foreach (var cval in row) colList[index++].Add(cval);

                                return colList;
                            }
                        );
                        index = 0;
                        foreach (var col in schema.Columns) col.Type.ValuesFromConst(cl[index++]);
                    }
                }
                else
                {
                    if (schema.Columns.Count != insertParser.valueList.Count())
                        throw new FormatException($"Value count mismatch. Server expected {schema.Columns.Count} and query contains {insertParser.valueList.Count()}.");

                    var valueList = insertParser.valueList as List<Parser.ValueType> ?? insertParser.valueList.ToList();
                    for (var i = 0; i < valueList.Count; i++)
                    {
                        var val = valueList[i];
                        if (val.TypeHint == Parser.ConstType.Parameter)
                            schema.Columns[i].Type.ValueFromParam(Parameters[val.StringValue]);
                        else
                            schema.Columns[i].Type.ValueFromConst(val);
                    }
                }

                await connection.Formatter.SendBlocksAsync(new[] { schema }).ConfigureAwait(false);
            }
            else
            {
                await connection.Formatter.RunQueryAsync(SubstituteParameters(CommandText), QueryProcessingStage.Complete, null, null, null, false).ConfigureAwait(false);
            }

            if (!readResponse) return;
            await connection.Formatter.ReadResponseAsync().ConfigureAwait(false);
        }

        private static readonly Regex ParamRegex = new Regex("[@:](?<n>([a-z_][a-z0-9_]*)|[@:])", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private string SubstituteParameters(string commandText) =>
            ParamRegex.Replace(commandText, m => m.Groups["n"].Value == ":" || m.Groups["n"].Value == "@" ? m.Groups["n"].Value : Parameters[m.Groups["n"].Value].AsSubstitute());

        public int ExecuteNonQuery()
        {
            return ExecuteNonQueryAsync().Result;
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            await ExecuteAsync(true, Connection).ConfigureAwait(false);
            return 0;
        }
#if NETCOREAPP11
        public ClickHouseDataReader ExecuteReader()
        {
            Execute(false);
            return new ClickHouseDataReader(_clickHouseConnection);
        }
#else
        public IDataReader ExecuteReader() => ExecuteReaderAsync().Result;

        public Task<ClickHouseDataReader> ExecuteReaderAsync() => ExecuteReaderAsync(CommandBehavior.Default);

        public async Task<ClickHouseDataReader> ExecuteReaderAsync(CommandBehavior behavior)
        {
            var tempConnection = Connection;
            await ExecuteAsync(false, tempConnection).ConfigureAwait(false);
            return await ClickHouseDataReader.Factory.CreateReaderAsync(tempConnection, behavior).ConfigureAwait(false);
        }

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            return ExecuteReaderAsync(behavior).Result;
        }
#endif

        public object ExecuteScalar()
        {
            return ExecuteScalarAsync().Result;
        }

        public async Task<object> ExecuteScalarAsync()
        {
            object result = null;
            using (var reader = await ExecuteReaderAsync().ConfigureAwait(false))
            {
                do
                {
                    if (!reader.Read()) continue;
                    result = reader.GetValue(0);
                } while (await reader.NextResultAsync().ConfigureAwait(false));
            }

            return result;
        }

        public ClickHouseConnection Connection { get; set; }

        public string CommandText { get; set; }
        public int CommandTimeout { get; set; }
        public ClickHouseParameterCollection Parameters { get; } = new ClickHouseParameterCollection();
    }
}