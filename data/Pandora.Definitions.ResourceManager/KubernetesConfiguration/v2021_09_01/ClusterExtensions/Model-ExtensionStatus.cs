using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Pandora.Definitions.Attributes;
using Pandora.Definitions.Attributes.Validation;
using Pandora.Definitions.CustomTypes;

namespace Pandora.Definitions.ResourceManager.KubernetesConfiguration.v2021_09_01.ClusterExtensions;


internal class ExtensionStatusModel
{
    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("displayStatus")]
    public string? DisplayStatus { get; set; }

    [JsonPropertyName("level")]
    public LevelTypeConstant? Level { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("time")]
    public string? Time { get; set; }
}
