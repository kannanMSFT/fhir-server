// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Abstractions.Data;
using Microsoft.Health.Abstractions.Features.Events;
using Microsoft.Health.Core.Features.Events;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Models;

namespace Microsoft.Health.Fhir.Core.Features.Operations.PublishEvents
{
    public class PublishEventsWorker : IPublishEventsWorker
    {
        private readonly ISource<IResourceChangeData> _fhirResourcesChangeFeedStore;
        private readonly ISink<IEvent> _eventSink;
        private readonly PublishEventsConfiguration _publishEventsConfiguration;
        private readonly ILogger _logger;

        /// <summary>
        /// Publish Events Job worker.
        /// </summary>
        /// <param name="fhirResourcesChangeFeedStore">Source Data Store</param>
        /// <param name="eventSink">Sink for the events to be written to.</param>
        /// <param name="publishEventsJobConfiguration">Configuration</param>
        /// <param name="logger">Logger</param>
        public PublishEventsWorker(
            ISource<IResourceChangeData> fhirResourcesChangeFeedStore,
            ISink<IEvent> eventSink,
            IOptions<PublishEventsConfiguration> publishEventsJobConfiguration,
            ILogger<PublishEventsWorker> logger)
        {
            _fhirResourcesChangeFeedStore = fhirResourcesChangeFeedStore;
            _eventSink = eventSink;
            _publishEventsConfiguration = publishEventsJobConfiguration.Value;
            _logger = logger;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            int startIndex = 1;
            var resourceChangeTypeMap = new Dictionary<string, string>
            {
                { "Creation", "Microsoft.HealthcareApis.FhirResourceCreated" },
                { "Update", "Microsoft.HealthcareApis.FhirResourceUpdated" },
                { "Deletion", "Microsoft.HealthcareApis.FhirResourceDeleted" },
            };

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Query DB and get records to publish
                    IReadOnlyCollection<IResourceChangeData> records =
                        await _fhirResourcesChangeFeedStore.FetchRecordsAsync(startIndex, 25, 1000, cancellationToken);

                    if (records.Count > 0)
                    {
                        // Publish events.
                        var events = records.Select(r => new EventData()
                        {
                            Topic = _publishEventsConfiguration.EventGridTopic,
                            Subject = $"{_publishEventsConfiguration.FhirAccount}/{r.ResourceType}/{r.ResourceId}",
                            EventType = resourceChangeTypeMap[r.ResourceChangeType],
                            EventTime = r.Timestamp,
                            Id = r.Id.ToString(),
                            DataVersion = r.ResourceVersion.ToString(),
                            Data = new BinaryData(new
                            {
                                ResourceType = r.ResourceType,
                                ResourceFhirAccount = _publishEventsConfiguration.FhirAccount,
                                ResourceFhirId = r.ResourceId,
                                ResourceVersionId = r.ResourceVersion,
                            }),
                        }).ToList();

                        await _eventSink.WriteAsync(events);

                        int count = records.Count;
                        _logger.LogInformation($@"Published {count} records by reading change feed ");

                        // Update watermark.
                        startIndex += records.Count;
                    }

                    await Task.Delay(_publishEventsConfiguration.JobPollingFrequency, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // End the execution of the task
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred on PublishEventsWorker.ExecuteAsync");
                }
            }
        }
    }
}
