// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Configs;

namespace Microsoft.Health.Fhir.Core.Features.Operations.PublishEvents
{
    public class PublishEventsWorker : IPublishEventsWorker
    {
        private readonly Func<IScoped<IFhirOperationDataStore>> _fhirOperationDataStoreFactory;
        private readonly PublishEventsConfiguration _publishEventsConfiguration;
        private readonly ILogger _logger;

        /// <summary>
        /// Publish Events Job worker.
        /// </summary>
        /// <param name="fhirOperationDataStoreFactory">Data Store</param>
        /// <param name="publishEventsJobConfiguration">Configuration</param>
        /// <param name="logger">Logger</param>
        public PublishEventsWorker(
            Func<IScoped<IFhirOperationDataStore>> fhirOperationDataStoreFactory,
            IOptions<PublishEventsConfiguration> publishEventsJobConfiguration,
            ILogger<PublishEventsWorker> logger)
        {
            _fhirOperationDataStoreFactory = fhirOperationDataStoreFactory;
            _publishEventsConfiguration = publishEventsJobConfiguration.Value;
            _logger = logger;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Query DB
                    // Get records to publish
                    // Publish events.

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
