// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Abstractions.Data;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.SqlServer;

namespace Microsoft.Health.Fhir.SqlServer.Features.ChangeFeed
{
    public class SqlResourceChangeDataReadOnlyDataStore : ISource<IResourceChangeData>
    {
        private readonly ISqlConnectionFactory _sqlConnectionFactory;
        private readonly ILogger<SqlResourceChangeDataReadOnlyDataStore> _logger;

        public SqlResourceChangeDataReadOnlyDataStore(ISqlConnectionFactory sqlConnectionFactory, ILogger<SqlResourceChangeDataReadOnlyDataStore> logger)
        {
            EnsureArg.IsNotNull(sqlConnectionFactory, nameof(sqlConnectionFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _sqlConnectionFactory = sqlConnectionFactory;
            _logger = logger;
        }

        public async Task<IReadOnlyCollection<IResourceChangeData>> FetchRecordsAsync(long startId, int pageSize, int? delayMilliseconds, CancellationToken cancellationToken)
        {
            EnsureArg.IsGte(startId, 0, nameof(startId));
            EnsureArg.IsGte(pageSize, 0, nameof(pageSize));

            var listResourceChangeData = new List<ResourceChangeData>();
            try
            {
                using (SqlConnection sqlConnection = await _sqlConnectionFactory.GetSqlConnectionAsync(cancellationToken: cancellationToken))
                {
                    await sqlConnection.OpenAsync(cancellationToken);
                    using (SqlCommand sqlCommand = new SqlCommand("dbo.FetchResourceChangeData", sqlConnection))
                    {
                        sqlCommand.CommandType = CommandType.StoredProcedure;
                        sqlCommand.Parameters.AddWithValue("@startId", SqlDbType.BigInt).Value = startId;
                        sqlCommand.Parameters.AddWithValue("@pageSize", SqlDbType.Int).Value = pageSize;
                        if (delayMilliseconds.HasValue)
                        {
                            sqlCommand.Parameters.AddWithValue("@delayMilliseconds", SqlDbType.Int).Value = delayMilliseconds;
                        }

                        using (SqlDataReader sqlDataReader = await sqlCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken))
                        {
                            while (await sqlDataReader.ReadAsync(cancellationToken))
                            {
                                listResourceChangeData.Add(new ResourceChangeData
                                {
                                    Id = (long)sqlDataReader["Id"],
                                    Timestamp = DateTime.SpecifyKind((DateTime)sqlDataReader["Timestamp"], DateTimeKind.Utc),
                                    ResourceId = (string)sqlDataReader["ResourceId"],
                                    ResourceType = (string)sqlDataReader["ResourceType"],
                                    ResourceVersion = (int)sqlDataReader["ResourceVersion"],
                                    ResourceChangeType = (string)sqlDataReader["ResourceChangeType"],
                                });
                            }
                        }

                        return listResourceChangeData;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error from SQL database on FetchResourceChangeData");
                throw;
            }
        }
    }
}
