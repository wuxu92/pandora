using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Relay.v2021_11_01.NamespacesPrivateLinkResources;

internal class Definition : ResourceDefinition
{
    public string Name => "NamespacesPrivateLinkResources";
    public IEnumerable<Interfaces.ApiOperation> Operations => new List<Interfaces.ApiOperation>
    {
        new PrivateLinkResourcesGetOperation(),
        new PrivateLinkResourcesListOperation(),
    };
}
