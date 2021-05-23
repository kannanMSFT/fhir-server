// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.SqlServer.Features.ChangeFeed.Model
{
    public interface IResourceChangeData
    {
        long Id { get; set; }

        DateTime Timestamp { get; set; }

        string ResourceId { get; set; }

        short ResourceTypeId { get; set; }

        int ResourceVersion { get; set; }

        short ResourceChangeTypeId { get; set; }
    }
}
