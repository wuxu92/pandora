using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Pandora.Definitions.Attributes;
using Pandora.Definitions.Attributes.Validation;
using Pandora.Definitions.CustomTypes;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.ServiceBus.v2021_06_01_preview.Topics;


internal class SBTopicPropertiesModel
{
    [DateFormat(DateFormatAttribute.DateFormat.RFC3339)]
    [JsonPropertyName("accessedAt")]
    public DateTime? AccessedAt { get; set; }

    [JsonPropertyName("autoDeleteOnIdle")]
    public string? AutoDeleteOnIdle { get; set; }

    [JsonPropertyName("countDetails")]
    public MessageCountDetailsModel? CountDetails { get; set; }

    [DateFormat(DateFormatAttribute.DateFormat.RFC3339)]
    [JsonPropertyName("createdAt")]
    public DateTime? CreatedAt { get; set; }

    [JsonPropertyName("defaultMessageTimeToLive")]
    public string? DefaultMessageTimeToLive { get; set; }

    [JsonPropertyName("duplicateDetectionHistoryTimeWindow")]
    public string? DuplicateDetectionHistoryTimeWindow { get; set; }

    [JsonPropertyName("enableBatchedOperations")]
    public bool? EnableBatchedOperations { get; set; }

    [JsonPropertyName("enableExpress")]
    public bool? EnableExpress { get; set; }

    [JsonPropertyName("enablePartitioning")]
    public bool? EnablePartitioning { get; set; }

    [JsonPropertyName("maxMessageSizeInKilobytes")]
    public int? MaxMessageSizeInKilobytes { get; set; }

    [JsonPropertyName("maxSizeInMegabytes")]
    public int? MaxSizeInMegabytes { get; set; }

    [JsonPropertyName("requiresDuplicateDetection")]
    public bool? RequiresDuplicateDetection { get; set; }

    [JsonPropertyName("sizeInBytes")]
    public int? SizeInBytes { get; set; }

    [JsonPropertyName("status")]
    public EntityStatusConstant? Status { get; set; }

    [JsonPropertyName("subscriptionCount")]
    public int? SubscriptionCount { get; set; }

    [JsonPropertyName("supportOrdering")]
    public bool? SupportOrdering { get; set; }

    [DateFormat(DateFormatAttribute.DateFormat.RFC3339)]
    [JsonPropertyName("updatedAt")]
    public DateTime? UpdatedAt { get; set; }
}
