using Pandora.Definitions.Attributes;
using System.ComponentModel;

namespace Pandora.Definitions.ResourceManager.NetApp.v2022_05_01.NetAppResource;

[ConstantType(ConstantTypeAttribute.ConstantType.String)]
internal enum InAvailabilityReasonTypeConstant
{
    [Description("AlreadyExists")]
    AlreadyExists,

    [Description("Invalid")]
    Invalid,
}
