using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.KubernetesConfiguration.v2022_11_01.Flux;

internal class FluxConfigurationId : ResourceID
{
    public string? CommonAlias => null;

    public string ID => "/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/{clusterRp}/{clusterResourceName}/{clusterName}/providers/Microsoft.KubernetesConfiguration/fluxConfigurations/{fluxConfigurationName}";

    public List<ResourceIDSegment> Segments => new List<ResourceIDSegment>
    {
        ResourceIDSegment.Static("staticSubscriptions", "subscriptions"),
        ResourceIDSegment.SubscriptionId("subscriptionId"),
        ResourceIDSegment.Static("staticResourceGroups", "resourceGroups"),
        ResourceIDSegment.ResourceGroup("resourceGroupName"),
        ResourceIDSegment.Static("staticProviders", "providers"),
        ResourceIDSegment.UserSpecified("clusterRp"),
        ResourceIDSegment.UserSpecified("clusterResourceName"),
        ResourceIDSegment.UserSpecified("clusterName"),
        ResourceIDSegment.Static("staticProviders2", "providers"),
        ResourceIDSegment.ResourceProvider("staticMicrosoftKubernetesConfiguration", "Microsoft.KubernetesConfiguration"),
        ResourceIDSegment.Static("staticFluxConfigurations", "fluxConfigurations"),
        ResourceIDSegment.UserSpecified("fluxConfigurationName"),
    };
}
