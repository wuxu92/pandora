using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.Automation.v2022_08_08.SoftwareUpdateConfigurationRun;

internal class Definition : ResourceDefinition
{
    public string Name => "SoftwareUpdateConfigurationRun";
    public IEnumerable<Interfaces.ApiOperation> Operations => new List<Interfaces.ApiOperation>
    {
        new SoftwareUpdateConfigurationRunsGetByIdOperation(),
        new SoftwareUpdateConfigurationRunsListOperation(),
    };
}
