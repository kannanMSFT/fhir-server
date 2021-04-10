﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Core.Features.Operations.Import
{
    public class BulkRawResourceProcessor
    {
        internal const int MaxBatchSize = 1000;
        internal static readonly int MaxConcurrentCount = Environment.ProcessorCount * 2;

        private IBulkImportDataExtractor _bulkImportDataExtractor;

        public BulkRawResourceProcessor(IBulkImportDataExtractor bulkImportDataExtractor)
        {
            _bulkImportDataExtractor = bulkImportDataExtractor;
        }

        public async Task ProcessingDataAsync(Channel<string> inputChannel, Channel<BulkImportResourceWrapper> outputChannel, long startSurrogateId, CancellationToken cancellationToken)
        {
            List<string> buffer = new List<string>();
            Queue<Task<IEnumerable<BulkImportResourceWrapper>>> processingTasks = new Queue<Task<IEnumerable<BulkImportResourceWrapper>>>();

            while (await inputChannel.Reader.WaitToReadAsync() && !cancellationToken.IsCancellationRequested)
            {
                await foreach (string rawData in inputChannel.Reader.ReadAllAsync())
                {
                    buffer.Add(rawData);
                    if (buffer.Count < MaxBatchSize)
                    {
                        continue;
                    }

                    while (processingTasks.Count >= MaxConcurrentCount)
                    {
                        IEnumerable<BulkImportResourceWrapper> headTaskResults = await processingTasks.Dequeue();
                        foreach (BulkImportResourceWrapper resourceWrapper in headTaskResults)
                        {
                            await outputChannel.Writer.WriteAsync(resourceWrapper);
                        }
                    }

                    string[] rawResources = buffer.ToArray();
                    buffer.Clear();
                    processingTasks.Enqueue(ProcessRawResources(rawResources, startSurrogateId));
                    startSurrogateId += rawResources.Length;
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    processingTasks.Enqueue(ProcessRawResources(buffer.ToArray(), startSurrogateId));
                    while (processingTasks.Count > 0)
                    {
                        IEnumerable<BulkImportResourceWrapper> headTaskResults = await processingTasks.Dequeue();
                        foreach (BulkImportResourceWrapper resourceWrapper in headTaskResults)
                        {
                            await outputChannel.Writer.WriteAsync(resourceWrapper);
                        }
                    }
                }

                outputChannel.Writer.Complete();
            }
        }

        private async Task<IEnumerable<BulkImportResourceWrapper>> ProcessRawResources(string[] rawResources, long startSurrogateId)
        {
            return await Task.Run(() =>
            {
                List<BulkImportResourceWrapper> result = new List<BulkImportResourceWrapper>();

                foreach (string rawData in rawResources)
                {
                    BulkImportResourceWrapper resourceWrapper = _bulkImportDataExtractor.GetBulkImportResourceWrapper(rawData);
                    resourceWrapper.ResourceSurrogateId = startSurrogateId++;
                    result.Add(resourceWrapper);
                }

                return result;
            });
        }
    }
}