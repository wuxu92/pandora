using Pandora.Definitions.Attributes;
using System.ComponentModel;

namespace Pandora.Definitions.ResourceManager.ContainerService.v2022_09_02_preview.AgentPools;

[ConstantType(ConstantTypeAttribute.ConstantType.String)]
internal enum WorkloadRuntimeConstant
{
    [Description("OCIContainer")]
    OCIContainer,

    [Description("WasmWasi")]
    WasmWasi,
}
