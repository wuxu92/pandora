using Pandora.Definitions.Attributes;
using Pandora.Definitions.CustomTypes;
using Pandora.Definitions.Interfaces;
using Pandora.Definitions.Operations;
using System;
using System.Collections.Generic;
using System.Net;


// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.


namespace Pandora.Definitions.ResourceManager.SecurityInsights.v2022_11_01.IncidentRelations;

internal class ListOperation : Operations.ListOperation
{
    public override string? FieldContainingPaginationDetails() => "nextLink";

    public override ResourceID? ResourceId() => new IncidentId();

    public override Type NestedItemType() => typeof(RelationModel);

    public override Type? OptionsObject() => typeof(ListOperation.ListOptions);

    public override string? UriSuffix() => "/relations";

    internal class ListOptions
    {
        [QueryStringName("$filter")]
        [Optional]
        public string Filter { get; set; }

        [QueryStringName("$orderby")]
        [Optional]
        public string Orderby { get; set; }

        [QueryStringName("$top")]
        [Optional]
        public int Top { get; set; }
    }
}
