// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Core.Models
{
    public interface IResourceChangeData
    {
        long Id { get; set; }

        DateTime Timestamp { get; set; }

        string ResourceId { get; set; }

        string ResourceType { get; set; }

        int ResourceVersion { get; set; }

        string ResourceChangeType { get; set; }
    }
}
