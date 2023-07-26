using Pandora.Definitions.Attributes;
using System.ComponentModel;

namespace Pandora.Definitions.ResourceManager.Network.v2023_04_01.VirtualNetworks;

[ConstantType(ConstantTypeAttribute.ConstantType.String)]
internal enum IsWorkloadProtectedConstant
{
    [Description("False")]
    False,

    [Description("True")]
    True,
}
