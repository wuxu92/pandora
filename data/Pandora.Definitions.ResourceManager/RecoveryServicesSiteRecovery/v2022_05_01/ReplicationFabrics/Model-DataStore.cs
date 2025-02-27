using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Pandora.Definitions.Attributes;
using Pandora.Definitions.Attributes.Validation;
using Pandora.Definitions.CustomTypes;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.RecoveryServicesSiteRecovery.v2022_05_01.ReplicationFabrics;


internal class DataStoreModel
{
    [JsonPropertyName("capacity")]
    public string? Capacity { get; set; }

    [JsonPropertyName("freeSpace")]
    public string? FreeSpace { get; set; }

    [JsonPropertyName("symbolicName")]
    public string? SymbolicName { get; set; }

    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("uuid")]
    public string? Uuid { get; set; }
}
