// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Features.Operations.PublishEvents;
using Microsoft.Health.Fhir.CosmosDb.Features.Storage;

namespace Microsoft.Health.Fhir.Api.Features.Operations.PublishEvents
{
    /// <summary>
    /// Background Service to publish events.
    /// </summary>
    public class PublishEventsBackgroundService : BackgroundService
    {
        private readonly ICosmosDbDistributedLock _distributedLock;
        private readonly IPublishEventsWorker _publishEventsWorker;
        private readonly PublishEventsConfiguration _publishEventsConfiguration;

        /// <summary>
        /// Constructor for Background Service to publish events
        /// </summary>
        /// <param name="distributedLock">distributed lock</param>
        /// <param name="publishEventsWorker">Publish Events Worker</param>
        /// <param name="publishEventsJobConfiguration">Publish Events Configuration</param>
        public PublishEventsBackgroundService(
            ICosmosDbDistributedLock distributedLock,
            IPublishEventsWorker publishEventsWorker,
            IOptions<PublishEventsConfiguration> publishEventsJobConfiguration)
        {
            EnsureArg.IsNotNull(publishEventsWorker, nameof(publishEventsWorker));
            EnsureArg.IsNotNull(publishEventsJobConfiguration, nameof(publishEventsJobConfiguration));

            _distributedLock = distributedLock;
            _publishEventsWorker = publishEventsWorker;
            _publishEventsConfiguration = publishEventsJobConfiguration.Value;
        }

        /// <summary>
        ///  Executes the long running publish events job worker.
        /// </summary>
        /// <param name="stoppingToken">Cancellation token</param>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (_publishEventsConfiguration.Enabled)
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    if (await _distributedLock.TryAcquireLock())
                    {
                        await _publishEventsWorker.ExecuteAsync(stoppingToken);
                    }

                    await Task.Delay(_publishEventsConfiguration.JobPollingFrequency, stoppingToken);
                }

                await _distributedLock.ReleaseLock();
            }
        }
    }
}
