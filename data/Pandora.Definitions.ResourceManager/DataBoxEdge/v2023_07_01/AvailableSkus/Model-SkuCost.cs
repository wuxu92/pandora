using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Pandora.Definitions.Attributes;
using Pandora.Definitions.Attributes.Validation;
using Pandora.Definitions.CustomTypes;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.DataBoxEdge.v2023_07_01.AvailableSkus;


internal class SkuCostModel
{
    [JsonPropertyName("extendedUnit")]
    public string? ExtendedUnit { get; set; }

    [JsonPropertyName("meterId")]
    public string? MeterId { get; set; }

    [JsonPropertyName("quantity")]
    public int? Quantity { get; set; }
}
