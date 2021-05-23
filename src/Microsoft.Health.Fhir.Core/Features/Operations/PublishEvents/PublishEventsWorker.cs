// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Abstractions.Data;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Models;

namespace Microsoft.Health.Fhir.Core.Features.Operations.PublishEvents
{
    public class PublishEventsWorker : IPublishEventsWorker
    {
        private readonly ISource<IResourceChangeData> _fhirResourcesChangeFeedStore;
        private readonly PublishEventsConfiguration _publishEventsConfiguration;
        private readonly ILogger _logger;

        /// <summary>
        /// Publish Events Job worker.
        /// </summary>
        /// <param name="fhirResourcesChangeFeedStore">Data Store</param>
        /// <param name="publishEventsJobConfiguration">Configuration</param>
        /// <param name="logger">Logger</param>
        public PublishEventsWorker(
            ISource<IResourceChangeData> fhirResourcesChangeFeedStore,
            IOptions<PublishEventsConfiguration> publishEventsJobConfiguration,
            ILogger<PublishEventsWorker> logger)
        {
            _fhirResourcesChangeFeedStore = fhirResourcesChangeFeedStore;
            _publishEventsConfiguration = publishEventsJobConfiguration.Value;
            _logger = logger;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            int startIndex = 1;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Query DB and get records to publish
                    IReadOnlyCollection<IResourceChangeData> records =
                        await _fhirResourcesChangeFeedStore.FetchRecordsAsync(startIndex, 25, 1000, cancellationToken);

                    // Publish events.
                    int count = records.Count;
                    _logger.LogInformation($@"Published {count} records by reading change feed ");

                    // Update watermark.
                    if (records.Count > 0)
                    {
                        startIndex += records.Count;
                    }

                    await Task.Delay(_publishEventsConfiguration.JobPollingFrequency, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // End the execution of the task
                }
            }
        }
    }
}
