using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Automation.v2022_08_08.SoftwareUpdateConfigurationMachineRun;

internal class SoftwareUpdateConfigurationMachineRunId : ResourceID
{
    public string? CommonAlias => null;

    public string ID => "/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Automation/automationAccounts/{automationAccountName}/softwareUpdateConfigurationMachineRuns/{softwareUpdateConfigurationMachineRunId}";

    public List<ResourceIDSegment> Segments => new List<ResourceIDSegment>
    {
        ResourceIDSegment.Static("staticSubscriptions", "subscriptions"),
        ResourceIDSegment.SubscriptionId("subscriptionId"),
        ResourceIDSegment.Static("staticResourceGroups", "resourceGroups"),
        ResourceIDSegment.ResourceGroup("resourceGroupName"),
        ResourceIDSegment.Static("staticProviders", "providers"),
        ResourceIDSegment.ResourceProvider("staticMicrosoftAutomation", "Microsoft.Automation"),
        ResourceIDSegment.Static("staticAutomationAccounts", "automationAccounts"),
        ResourceIDSegment.UserSpecified("automationAccountName"),
        ResourceIDSegment.Static("staticSoftwareUpdateConfigurationMachineRuns", "softwareUpdateConfigurationMachineRuns"),
        ResourceIDSegment.UserSpecified("softwareUpdateConfigurationMachineRunId"),
    };
}
