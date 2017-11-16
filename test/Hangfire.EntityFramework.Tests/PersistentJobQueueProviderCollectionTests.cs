// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Linq;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class PersistentJobQueueProviderCollectionTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenDefaultProviderIsNull()
        {
            Assert.Throws<ArgumentNullException>("defaultProvider",
                () => new PersistentJobQueueProviderCollection(null));
        }

        [Fact]
        public void Ctor_SavesDefaultProviderCorrectly()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();

            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);

            Assert.Same(defaultProvider.Object, collection.DefaultProvider);
        }

        [Fact]
        public void Add_ThrowsAnException_WhenProviderIsNull()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);

            Assert.Throws<ArgumentNullException>("provider", () => collection.Add(null, new string[0]));
        }

        [Fact]
        public void Add_ThrowsAnException_WhenQueuesIsNull()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);

            Assert.Throws<ArgumentNullException>("queues", () => collection.Add(defaultProvider.Object, null));
        }

        [Fact]
        public void GetProvider_ThrowsAnException_WhenQueueIsNull()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);

            Assert.Throws<ArgumentNullException>("queue", () => collection.GetProvider(null));
        }

        [Fact]
        public void GetProvider_ReturnsDefaultProvider_WhenQueueIsNotMapped()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);

            var provider = collection.GetProvider(string.Empty);

            Assert.Same(defaultProvider.Object, provider);
        }

        [Fact]
        public void GetProvider_ReturnsCorrectProvider_WhenQueueIsMapped()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            Mock<IPersistentJobQueueProvider> otherProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);
            collection.Add(otherProvider.Object, new[] { "QUEUE" });

            var provider = collection.GetProvider("QUEUE");

            Assert.Same(otherProvider.Object, provider);
        }

        [Fact]
        public void GetEnumerator_ReturnsDefaultProvider_WhenOtherProviderNotSet()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);

            var result = collection.ToArray();
            var nonGenericResult = ((IEnumerable)collection).Cast<object>().ToArray();

            Assert.Single(result);
            Assert.Equal(result, nonGenericResult);
            Assert.Equal(defaultProvider.Object, result.Single());
        }

        [Fact]
        public void GetEnumerator_ReturnsDistinctProviders_WhenMulipleOtherProvidersSet()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            Mock<IPersistentJobQueueProvider> otherProvider = new Mock<IPersistentJobQueueProvider>();
            var collection = new PersistentJobQueueProviderCollection(defaultProvider.Object);
            collection.Add(defaultProvider.Object, new[] { "DEFAULT" });
            collection.Add(otherProvider.Object, new[] { "OTHER1", "OTHER2" });

            var result = collection.ToArray();
            var nonGenericResult = ((IEnumerable)collection).Cast<object>().ToArray();

            Assert.Equal(2, result.Length);
            Assert.Equal(result, nonGenericResult);
            Assert.Contains(defaultProvider.Object, result);
            Assert.Contains(otherProvider.Object, result);
        }
    }
}
