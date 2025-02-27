using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.ServiceFabricManagedCluster.v2022_01_01.ManagedVMSizes;

internal class ManagedUnsupportedVMSizeId : ResourceID
{
    public string? CommonAlias => null;

    public string ID => "/subscriptions/{subscriptionId}/providers/Microsoft.ServiceFabric/locations/{location}/managedUnsupportedVMSizes/{vmSize}";

    public List<ResourceIDSegment> Segments => new List<ResourceIDSegment>
    {
        ResourceIDSegment.Static("staticSubscriptions", "subscriptions"),
        ResourceIDSegment.SubscriptionId("subscriptionId"),
        ResourceIDSegment.Static("staticProviders", "providers"),
        ResourceIDSegment.ResourceProvider("staticMicrosoftServiceFabric", "Microsoft.ServiceFabric"),
        ResourceIDSegment.Static("staticLocations", "locations"),
        ResourceIDSegment.UserSpecified("location"),
        ResourceIDSegment.Static("staticManagedUnsupportedVMSizes", "managedUnsupportedVMSizes"),
        ResourceIDSegment.UserSpecified("vmSize"),
    };
}
