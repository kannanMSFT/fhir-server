﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using MediatR;

namespace Microsoft.Health.Fhir.Core.Messages.BulkImport
{
    public class GetBulkImportRequest : IRequest<GetBulkImportResponse>
    {
        public GetBulkImportRequest(Uri requestUri, string jobId)
        {
            EnsureArg.IsNotNull(requestUri, nameof(requestUri));
            EnsureArg.IsNotNullOrWhiteSpace(jobId, nameof(jobId));

            RequestUri = requestUri;
            JobId = jobId;
        }

        public Uri RequestUri { get; }

        public string JobId { get; }
    }
}
