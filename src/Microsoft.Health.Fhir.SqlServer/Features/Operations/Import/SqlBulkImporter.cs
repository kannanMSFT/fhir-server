﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Core.Features.Operations;
using Microsoft.Health.Fhir.Core.Features.Operations.Import;
using Microsoft.Health.Fhir.SqlServer.Features.Operations.Import.DataGenerator;

namespace Microsoft.Health.Fhir.SqlServer.Features.Operations.Import
{
    internal class SqlBulkImporter : IBulkImporter<BulkImportResourceWrapper>
    {
        private const int DefaultMaxResourceCountInBatch = 10000;
        private const int DefaultMaxConcurrentCount = 3;

        private List<TableBulkCopyDataGenerator<SqlBulkCopyDataWrapper>> _generators = new List<TableBulkCopyDataGenerator<SqlBulkCopyDataWrapper>>();
        private ISqlBulkCopyDataWrapperFactory _sqlBulkCopyDataWrapperFactory;
        private IFhirDataBulkOperation _fhirDataBulkOperation;

        public SqlBulkImporter(
            IFhirDataBulkOperation fhirDataBulkOperation,
            ISqlBulkCopyDataWrapperFactory sqlBulkCopyDataWrapperFactory,
            List<TableBulkCopyDataGenerator<SqlBulkCopyDataWrapper>> generators)
        {
            _fhirDataBulkOperation = fhirDataBulkOperation;
            _sqlBulkCopyDataWrapperFactory = sqlBulkCopyDataWrapperFactory;
            _generators = generators;
        }

        public SqlBulkImporter(
            IFhirDataBulkOperation fhirDataBulkOperation,
            ISqlBulkCopyDataWrapperFactory sqlBulkCopyDataWrapperFactory,
            ResourceTableBulkCopyDataGenerator resourceTableBulkCopyDataGenerator,
            CompartmentAssignmentTableBulkCopyDataGenerator compartmentAssignmentTableBulkCopyDataGenerator,
            ResourceWriteClaimTableBulkCopyDataGenerator resourceWriteClaimTableBulkCopyDataGenerator,
            DateTimeSearchParamsTableBulkCopyDataGenerator dateTimeSearchParamsTableBulkCopyDataGenerator,
            NumberSearchParamsTableBulkCopyDataGenerator numberSearchParamsTableBulkCopyDataGenerator,
            QuantitySearchParamsTableBulkCopyDataGenerator quantitySearchParamsTableBulkCopyDataGenerator,
            ReferenceSearchParamsTableBulkCopyDataGenerator referenceSearchParamsTableBulkCopyDataGenerator,
            ReferenceTokenCompositeSearchParamsTableBulkCopyDataGenerator referenceTokenCompositeSearchParamsTableBulkCopyDataGenerator,
            StringSearchParamsTableBulkCopyDataGenerator stringSearchParamsTableBulkCopyDataGenerator,
            TokenDateTimeCompositeSearchParamsTableBulkCopyDataGenerator tokenDateTimeCompositeSearchParamsTableBulkCopyDataGenerator,
            TokenNumberNumberCompositeSearchParamsTableBulkCopyDataGenerator tokenNumberNumberCompositeSearchParamsTableBulkCopyDataGenerator,
            TokenQuantityCompositeSearchParamsTableBulkCopyDataGenerator tokenQuantityCompositeSearchParamsTableBulkCopyDataGenerator,
            TokenSearchParamsTableBulkCopyDataGenerator tokenSearchParamsTableBulkCopyDataGenerator,
            TokenStringCompositeSearchParamsTableBulkCopyDataGenerator tokenStringCompositeSearchParamsTableBulkCopyDataGenerator,
            TokenTextSearchParamsTableBulkCopyDataGenerator tokenTextSearchParamsTableBulkCopyDataGenerator,
            TokenTokenCompositeSearchParamsTableBulkCopyDataGenerator tokenTokenCompositeSearchParamsTableBulkCopyDataGenerator,
            UriSearchParamsTableBulkCopyDataGenerator uriSearchParamsTableBulkCopyDataGenerator)
        {
            _fhirDataBulkOperation = fhirDataBulkOperation;
            _sqlBulkCopyDataWrapperFactory = sqlBulkCopyDataWrapperFactory;

            _generators.Add(resourceTableBulkCopyDataGenerator);
            _generators.Add(compartmentAssignmentTableBulkCopyDataGenerator);
            _generators.Add(resourceWriteClaimTableBulkCopyDataGenerator);
            _generators.Add(dateTimeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(numberSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(quantitySearchParamsTableBulkCopyDataGenerator);
            _generators.Add(referenceSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(referenceTokenCompositeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(stringSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenDateTimeCompositeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenNumberNumberCompositeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenQuantityCompositeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenStringCompositeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenTextSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(tokenTokenCompositeSearchParamsTableBulkCopyDataGenerator);
            _generators.Add(uriSearchParamsTableBulkCopyDataGenerator);
        }

        public int MaxResourceCountInBatch { get; set; } = DefaultMaxResourceCountInBatch;

        public int MaxConcurrentCount { get; set; } = DefaultMaxConcurrentCount;

        public async Task<long> ImportResourceAsync(Channel<BulkImportResourceWrapper> inputChannel, Action<(string tableName, long endSurrogateId)> progressUpdateAction,  CancellationToken cancellationToken)
        {
            long importedResourceCount = 0;
            Dictionary<string, DataTable> buffer = new Dictionary<string, DataTable>();
            Queue<Task<(string tableName, long endSurrogateId, long count)>> runningTasks = new Queue<Task<(string, long, long)>>();

            long surrogateId = 0;

            do
            {
                await foreach (BulkImportResourceWrapper resource in inputChannel.Reader.ReadAllAsync())
                {
                    surrogateId = resource.ResourceSurrogateId;

                    SqlBulkCopyDataWrapper dataWrapper = _sqlBulkCopyDataWrapperFactory.CreateSqlBulkCopyDataWrapper(resource);

                    foreach (TableBulkCopyDataGenerator<SqlBulkCopyDataWrapper> generator in _generators)
                    {
                        if (!buffer.ContainsKey(generator.TableName))
                        {
                            buffer[generator.TableName] = generator.GenerateDataTable();
                        }

                        generator.FillDataTable(buffer[generator.TableName], dataWrapper);
                    }

                    string[] tableNameNeedImport = buffer.Where(r => r.Value.Rows.Count >= MaxResourceCountInBatch).Select(r => r.Key).ToArray();
                    foreach (string tableName in tableNameNeedImport)
                    {
                        while (runningTasks.Count() >= MaxConcurrentCount)
                        {
                            (string tableName, long endSurrogateId, long count) result = await runningTasks.Dequeue();
                            progressUpdateAction((result.tableName, result.endSurrogateId));
                            importedResourceCount += result.count;
                        }

                        DataTable inputTable = buffer[tableName];
                        buffer.Remove(tableName);

                        runningTasks.Enqueue(BulkCopyAsync(inputTable, surrogateId, cancellationToken));
                    }
                }
            }
            while (await inputChannel.Reader.WaitToReadAsync(cancellationToken) && !cancellationToken.IsCancellationRequested);

            string[] tableNames = buffer.Keys.ToArray();
            foreach (string tableName in tableNames)
            {
                DataTable inputTable = buffer[tableName];
                runningTasks.Enqueue(BulkCopyAsync(inputTable, surrogateId, cancellationToken));
            }

            while (runningTasks.Count() > 0 && !cancellationToken.IsCancellationRequested)
            {
                (string tableName, long endSurrogateId, long count) result = await runningTasks.Dequeue();
                progressUpdateAction((result.tableName, result.endSurrogateId));
                importedResourceCount += result.count;
            }

            return importedResourceCount;
        }

        private async Task<(string tableName, long endSurrogateId, long count)> BulkCopyAsync(DataTable inputTable, long endSurrogateId, CancellationToken cancellationToken)
        {
            await _fhirDataBulkOperation.BulkCopyDataAsync(inputTable, cancellationToken);

            return (inputTable.TableName, endSurrogateId, inputTable.Rows.Count);
        }
    }
}