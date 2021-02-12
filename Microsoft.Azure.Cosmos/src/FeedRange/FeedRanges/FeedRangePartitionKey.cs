// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Routing;
    using Microsoft.Azure.Documents.Routing;

    /// <summary>
    /// FeedRange that represents an exact Partition Key value.
    /// </summary>
    internal sealed class FeedRangePartitionKey : FeedRangeInternal
    {
        public PartitionKey PartitionKey { get; }

        public FeedRangePartitionKey(PartitionKey partitionKey)
        {
            this.PartitionKey = partitionKey;
        }

        public override Task<List<Documents.Routing.Range<string>>> GetEffectiveRangesAsync(
            IRoutingMapProvider routingMapProvider,
            string containerRid,
            Documents.PartitionKeyDefinition partitionKeyDefinition)
        {
            if (partitionKeyDefinition.Kind == Documents.PartitionKind.MultiHash)
            {
                List<IPartitionKeyComponent> currentPartitionKeyComponents = new List<IPartitionKeyComponent>(this.PartitionKey.InternalKey.Components);
                int unfilledPartitionKeys = partitionKeyDefinition.Paths.Count - this.PartitionKey.InternalKey.Components.Count;
                for (int i = 0; i < unfilledPartitionKeys; i++)
                {
                    currentPartitionKeyComponents.Add(new InfinityPartitionKeyComponent());
                }

                PartitionKeyInternal maxPartitionKey = new PartitionKeyInternal(currentPartitionKeyComponents);
                return Task.FromResult(
                    new List<Documents.Routing.Range<string>>
                    {
                        new Documents.Routing.Range<string>(this.PartitionKey.InternalKey.GetEffectivePartitionKeyString(partitionKeyDefinition), maxPartitionKey.GetEffectivePartitionKeyString(partitionKeyDefinition), true, false)
                    });
            }

            return Task.FromResult(
            new List<Documents.Routing.Range<string>>
            {
                    Documents.Routing.Range<string>.GetPointRange(
                        this.PartitionKey.InternalKey.GetEffectivePartitionKeyString(partitionKeyDefinition))
            });
        }

        public override async Task<IEnumerable<string>> GetPartitionKeyRangesAsync(
            IRoutingMapProvider routingMapProvider,
            string containerRid,
            Documents.PartitionKeyDefinition partitionKeyDefinition,
            CancellationToken cancellationToken)
        {
            string effectivePartitionKeyString = this.PartitionKey.InternalKey.GetEffectivePartitionKeyString(partitionKeyDefinition);
            Documents.PartitionKeyRange range = await routingMapProvider.TryGetRangeByEffectivePartitionKeyAsync(containerRid, effectivePartitionKeyString);
            return new List<string>() { range.Id };
        }

        public override void Accept(IFeedRangeVisitor visitor) => visitor.Visit(this);

        public override Task<TResult> AcceptAsync<TResult>(
            IFeedRangeAsyncVisitor<TResult> visitor,
            CancellationToken cancellationToken = default) => visitor.VisitAsync(this, cancellationToken);

        public override string ToString() => this.PartitionKey.InternalKey.ToJsonString();
    }
}
