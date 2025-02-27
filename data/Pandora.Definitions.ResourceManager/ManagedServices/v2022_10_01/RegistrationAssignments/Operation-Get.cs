using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.ManagedServices.v2022_10_01.RegistrationAssignments;

internal class GetOperation : Operations.GetOperation
{
    public override ResourceID? ResourceId() => new ScopedRegistrationAssignmentId();

    public override Type? ResponseObject() => typeof(RegistrationAssignmentModel);

    public override Type? OptionsObject() => typeof(GetOperation.GetOptions);

    internal class GetOptions
    {
        [QueryStringName("$expandRegistrationDefinition")]
        [Optional]
        public bool ExpandRegistrationDefinition { get; set; }
    }
}
