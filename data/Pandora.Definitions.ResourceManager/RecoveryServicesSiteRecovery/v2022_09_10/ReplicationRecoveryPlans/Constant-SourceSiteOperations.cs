using Pandora.Definitions.Attributes;
using System.ComponentModel;

namespace Pandora.Definitions.ResourceManager.RecoveryServicesSiteRecovery.v2022_09_10.ReplicationRecoveryPlans;

[ConstantType(ConstantTypeAttribute.ConstantType.String)]
internal enum SourceSiteOperationsConstant
{
    [Description("NotRequired")]
    NotRequired,

    [Description("Required")]
    Required,
}
