using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Pandora.Definitions.Attributes;
using Pandora.Definitions.Attributes.Validation;
using Pandora.Definitions.CustomTypes;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Compute.v2022_03_01.VirtualMachineRunCommands;


internal class RunCommandDocumentBaseModel
{
    [JsonPropertyName("description")]
    [Required]
    public string Description { get; set; }

    [JsonPropertyName("id")]
    [Required]
    public string Id { get; set; }

    [JsonPropertyName("label")]
    [Required]
    public string Label { get; set; }

    [JsonPropertyName("osType")]
    [Required]
    public OperatingSystemTypesConstant OsType { get; set; }

    [JsonPropertyName("$schema")]
    [Required]
    public string Schema { get; set; }
}
