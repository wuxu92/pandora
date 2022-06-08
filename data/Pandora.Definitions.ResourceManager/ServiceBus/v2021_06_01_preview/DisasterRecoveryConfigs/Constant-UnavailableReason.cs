using Pandora.Definitions.Attributes;
using System.ComponentModel;

namespace Pandora.Definitions.ResourceManager.ServiceBus.v2021_06_01_preview.DisasterRecoveryConfigs;

[ConstantType(ConstantTypeAttribute.ConstantType.String)]
internal enum UnavailableReasonConstant
{
    [Description("InvalidName")]
    InvalidName,

    [Description("NameInLockdown")]
    NameInLockdown,

    [Description("NameInUse")]
    NameInUse,

    [Description("None")]
    None,

    [Description("SubscriptionIsDisabled")]
    SubscriptionIsDisabled,

    [Description("TooManyNamespaceInCurrentSubscription")]
    TooManyNamespaceInCurrentSubscription,
}
