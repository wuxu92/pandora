using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Media.v2022_08_01.LiveEvents;

internal class AsyncOperationOperation : Operations.GetOperation
{
    public override ResourceID? ResourceId() => new LiveEventOperationId();

    public override Type? ResponseObject() => typeof(AsyncOperationResultModel);


}
