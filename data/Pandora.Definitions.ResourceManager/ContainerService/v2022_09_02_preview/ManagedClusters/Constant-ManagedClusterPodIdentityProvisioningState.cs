using Pandora.Definitions.Attributes;
using System.ComponentModel;

namespace Pandora.Definitions.ResourceManager.ContainerService.v2022_09_02_preview.ManagedClusters;

[ConstantType(ConstantTypeAttribute.ConstantType.String)]
internal enum ManagedClusterPodIdentityProvisioningStateConstant
{
    [Description("Assigned")]
    Assigned,

    [Description("Deleting")]
    Deleting,

    [Description("Failed")]
    Failed,

    [Description("Updating")]
    Updating,
}
