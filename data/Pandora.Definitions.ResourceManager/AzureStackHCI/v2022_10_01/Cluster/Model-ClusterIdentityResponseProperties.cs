using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Pandora.Definitions.Attributes;
using Pandora.Definitions.Attributes.Validation;
using Pandora.Definitions.CustomTypes;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.AzureStackHCI.v2022_10_01.Cluster;


internal class ClusterIdentityResponsePropertiesModel
{
    [JsonPropertyName("aadApplicationObjectId")]
    public string? AadApplicationObjectId { get; set; }

    [JsonPropertyName("aadClientId")]
    public string? AadClientId { get; set; }

    [JsonPropertyName("aadServicePrincipalObjectId")]
    public string? AadServicePrincipalObjectId { get; set; }

    [JsonPropertyName("aadTenantId")]
    public string? AadTenantId { get; set; }
}
