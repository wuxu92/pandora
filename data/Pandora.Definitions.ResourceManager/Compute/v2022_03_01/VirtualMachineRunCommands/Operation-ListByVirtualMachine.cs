using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Compute.v2022_03_01.VirtualMachineRunCommands;

internal class ListByVirtualMachineOperation : Operations.ListOperation
{
    public override string? FieldContainingPaginationDetails() => "nextLink";

    public override ResourceID? ResourceId() => new VirtualMachineId();

    public override Type NestedItemType() => typeof(VirtualMachineRunCommandModel);

    public override Type? OptionsObject() => typeof(ListByVirtualMachineOperation.ListByVirtualMachineOptions);

    public override string? UriSuffix() => "/runCommands";

    internal class ListByVirtualMachineOptions
    {
        [QueryStringName("$expand")]
        [Optional]
        public string Expand { get; set; }
    }
}
