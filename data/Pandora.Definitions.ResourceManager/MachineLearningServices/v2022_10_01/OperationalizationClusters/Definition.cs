using System.Collections.Generic;
using Pandora.Definitions.Interfaces;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.MachineLearningServices.v2022_10_01.OperationalizationClusters;

internal class Definition : ResourceDefinition
{
    public string Name => "OperationalizationClusters";
    public IEnumerable<Interfaces.ApiOperation> Operations => new List<Interfaces.ApiOperation>
    {
        new ComputeCreateOrUpdateOperation(),
        new ComputeDeleteOperation(),
        new ComputeGetOperation(),
        new ComputeListOperation(),
        new ComputeListKeysOperation(),
        new ComputeRestartOperation(),
        new ComputeStartOperation(),
        new ComputeStopOperation(),
        new ComputeUpdateOperation(),
    };
}
