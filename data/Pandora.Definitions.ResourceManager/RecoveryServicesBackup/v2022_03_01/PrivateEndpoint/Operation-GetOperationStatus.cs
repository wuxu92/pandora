using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.RecoveryServicesBackup.v2022_03_01.PrivateEndpoint;

internal class GetOperationStatusOperation : Operations.GetOperation
{
    public override ResourceID? ResourceId() => new OperationsStatusId();

    public override Type? ResponseObject() => typeof(OperationStatusModel);


}
