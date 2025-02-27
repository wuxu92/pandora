using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.FrontDoor.v2022_05_01.WebApplicationFirewallPolicies;

internal class Definition : ResourceDefinition
{
    public string Name => "WebApplicationFirewallPolicies";
    public IEnumerable<Interfaces.ApiOperation> Operations => new List<Interfaces.ApiOperation>
    {
        new PoliciesCreateOrUpdateOperation(),
        new PoliciesDeleteOperation(),
        new PoliciesGetOperation(),
        new PoliciesListOperation(),
        new PoliciesUpdateOperation(),
    };
}
