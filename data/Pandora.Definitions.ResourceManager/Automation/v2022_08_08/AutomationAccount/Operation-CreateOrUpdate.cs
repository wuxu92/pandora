using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Automation.v2022_08_08.AutomationAccount;

internal class CreateOrUpdateOperation : Operations.PutOperation
{
    public override Type? RequestObject() => typeof(AutomationAccountCreateOrUpdateParametersModel);

    public override ResourceID? ResourceId() => new AutomationAccountId();

    public override Type? ResponseObject() => typeof(AutomationAccountModel);


}
