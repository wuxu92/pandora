using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Network.v2023_04_01.VMSSPublicIPAddresses;

internal class PublicIPAddressesGetVirtualMachineScaleSetPublicIPAddressOperation : Pandora.Definitions.Operations.GetOperation
{
    public override ResourceID? ResourceId() => new VirtualMachineScaleSetPublicIPAddressId();

    public override Type? ResponseObject() => typeof(PublicIPAddressModel);

    public override Type? OptionsObject() => typeof(PublicIPAddressesGetVirtualMachineScaleSetPublicIPAddressOperation.PublicIPAddressesGetVirtualMachineScaleSetPublicIPAddressOptions);

    internal class PublicIPAddressesGetVirtualMachineScaleSetPublicIPAddressOptions
    {
        [QueryStringName("$expand")]
        [Optional]
        public string Expand { get; set; }
    }
}
