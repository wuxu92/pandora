using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.NetApp.v2022_05_01.SubVolumes;

internal class VolumeId : ResourceID
{
    public string? CommonAlias => null;

    public string ID => "/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.NetApp/netAppAccounts/{accountName}/capacityPools/{poolName}/volumes/{volumeName}";

    public List<ResourceIDSegment> Segments => new List<ResourceIDSegment>
    {
        ResourceIDSegment.Static("staticSubscriptions", "subscriptions"),
        ResourceIDSegment.SubscriptionId("subscriptionId"),
        ResourceIDSegment.Static("staticResourceGroups", "resourceGroups"),
        ResourceIDSegment.ResourceGroup("resourceGroupName"),
        ResourceIDSegment.Static("staticProviders", "providers"),
        ResourceIDSegment.ResourceProvider("staticMicrosoftNetApp", "Microsoft.NetApp"),
        ResourceIDSegment.Static("staticNetAppAccounts", "netAppAccounts"),
        ResourceIDSegment.UserSpecified("accountName"),
        ResourceIDSegment.Static("staticCapacityPools", "capacityPools"),
        ResourceIDSegment.UserSpecified("poolName"),
        ResourceIDSegment.Static("staticVolumes", "volumes"),
        ResourceIDSegment.UserSpecified("volumeName"),
    };
}
