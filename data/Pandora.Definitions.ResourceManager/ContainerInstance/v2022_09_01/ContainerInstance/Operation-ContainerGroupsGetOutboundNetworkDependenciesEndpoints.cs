using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.ContainerInstance.v2022_09_01.ContainerInstance;

internal class ContainerGroupsGetOutboundNetworkDependenciesEndpointsOperation : Operations.GetOperation
{
    public override ResourceID? ResourceId() => new ContainerGroupId();

    public override Type? ResponseObject() => typeof(List<string>);

    public override string? UriSuffix() => "/outboundNetworkDependenciesEndpoints";


}
