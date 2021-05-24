﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Core.Configs
{
    public class PublishEventsConfiguration
    {
        /// <summary>
        /// Determines whether publish events feature is enabled or not.
        /// </summary>
        public bool Enabled { get; set; }

        public TimeSpan JobPollingFrequency { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// EventEndPoint.
        /// </summary>
        public string EventEndPoint { get; set; }

        /// <summary>
        /// Access key for event grid.
        /// </summary>
        public string AccessKey { get; set; }
    }
}
