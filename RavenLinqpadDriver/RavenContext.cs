using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Reflection;
using System.Net;
using RavenLinqpadDriver.Common;
using Raven.Client.Documents.Session;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Linq;
using Raven.Client.Http;
using Raven.Client.Documents.Indexes;
using System.Threading;
using Raven.Client.Documents.BulkInsert;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;

namespace RavenLinqpadDriver
{
    public class RavenContext : IDocumentSession, IDocumentStore
    {
        private readonly IDocumentStore _docStore;
        private readonly Lazy<IDocumentSession> _lazySession;

        internal TextWriter LogWriter { get; set; }

        public RavenContext(RavenConnectionDialogViewModel connInfo)
        {
            if (connInfo == null) throw new ArgumentNullException("connInfo");

            _docStore = CreateDocStore(connInfo).Initialize();
            _lazySession = new Lazy<IDocumentSession>(_docStore.OpenSession);
            
            /* SetupLogWriting(); */
        }

        /*
        private void SetupLogWriting()
        {
            // sharded doc stores don't have a jsonrequestfactory,
            // so if sharding, get shard doc stores
            var shardedDocStore = _docStore as ShardedDocumentStore;
            if (shardedDocStore != null)
            {
                var docStores = from ds in shardedDocStore.ShardStrategy.Shards.Values
                                where !(ds is ShardedDocumentStore)
                                select ds;

                foreach (var docStore in docStores)
                    docStore.JsonRequestFactory.LogRequest += LogRequest;
            }
            else
            {
                _docStore.JsonRequestFactory.LogRequest += LogRequest;
            }
        }
        */

        /*
        void LogRequest(object sender, RequestResultArgs e)
        {
            if (LogWriter == null) return;

            var entry = new StringBuilder().AppendFormat(@"
{0} - {1}
Url: {2}
Duration: {3} milliseconds
Method: {4}
Posted Data: {5}
Http Result: {6}
Result Data: {7}
Total Size: {8:n0}",
                e.At,
                e.Status,
                e.Url,
                e.DurationMilliseconds,
                e.Method,
                e.PostedData,
                e.HttpResult,
                e.Result,
                e.TotalSize);

            foreach (var item in e.AdditionalInformation)
                entry.AppendFormat("{0}: {1}", item.Key, item.Value);

            entry.AppendLine();
            LogWriter.WriteLine(entry.ToString());
        }
        */

        private static IDocumentStore CreateDocStore(RavenConnectionDialogViewModel conn)
        {
            if (conn == null)
                throw new ArgumentNullException("conn", "conn is null.");

            var assemblies = conn.AssemblyPaths.Select(Path.GetFileNameWithoutExtension).Select(Assembly.Load);

            var docStoreCreatorType = (from a in assemblies
                                       from t in a.TypesImplementing<ICreateDocumentStore>()
                                       let hasDefaultCtor = t.GetConstructor(Type.EmptyTypes) != null
                                       where !t.IsAbstract && hasDefaultCtor
                                       select t).FirstOrDefault();

            if (docStoreCreatorType == null)
                return conn.CreateDocStore();

            var docStoreCreator = (ICreateDocumentStore)Activator.CreateInstance(docStoreCreatorType);

            var connectionInfo = new ConnectionInfo
            {
                Url = conn.Url,
                DefaultDatabase = conn.DefaultDatabase,
                Credentials = new NetworkCredential
                {
                    UserName = conn.Username,
                    Password = conn.Password
                },
                ResourceManagerId = conn.ResourceManagerId,
                ApiKey = conn.ApiKey
            };

            return docStoreCreator.CreateDocumentStore(connectionInfo);
        }

        public void Dispose()
        {
            if (_lazySession.IsValueCreated)
                _lazySession.Value.Dispose();

            if (_docStore != null && !_docStore.WasDisposed)
                _docStore.Dispose();
        }


        public bool WasDisposed
        {
            get { return _docStore.WasDisposed; }
        }

        public event EventHandler AfterDispose;
        public event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        public event EventHandler<AfterSaveChangesEventArgs> OnAfterSaveChanges;
        public event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;
        public event EventHandler<BeforeQueryEventArgs> OnBeforeQuery;
        public event EventHandler<SessionCreatedEventArgs> OnSessionCreated;
        public event EventHandler<BeforeConversionToDocumentEventArgs> OnBeforeConversionToDocument;
        public event EventHandler<AfterConversionToDocumentEventArgs> OnAfterConversionToDocument;
        public event EventHandler<BeforeConversionToEntityEventArgs> OnBeforeConversionToEntity;
        public event EventHandler<AfterConversionToEntityEventArgs> OnAfterConversionToEntity;
        public event EventHandler<FailedRequestEventArgs> OnFailedRequest;
        public event EventHandler<TopologyUpdatedEventArgs> OnTopologyUpdated;

        public IDatabaseChanges Changes(string database = null)
        {
            return _docStore.Changes(database);
        }

        public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
        {
            return _docStore.AggressivelyCacheFor(cacheDuration);
        }

        public IDisposable AggressivelyCache()
        {
            return _docStore.AggressivelyCache();
        }

        public IDisposable DisableAggressiveCaching()
        {
            return _docStore.DisableAggressiveCaching();
        }

        public IDocumentStore Initialize()
        {
            return _docStore.Initialize();
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            return _docStore.OpenAsyncSession();
        }

        public IAsyncDocumentSession OpenAsyncSession(string database)
        {
            return _docStore.OpenAsyncSession(database);
        }

        public IAsyncDocumentSession OpenAsyncSession(SessionOptions sessionOptions)
        {
            return _docStore.OpenAsyncSession(sessionOptions);
        }


        public IDocumentSession OpenSession()
        {
            return _docStore.OpenSession();
        }

        public IDocumentSession OpenSession(string database)
        {
            return _docStore.OpenSession(database);
        }

        public IDocumentSession OpenSession(SessionOptions sessionOptions)
        {
            return _docStore.OpenSession(sessionOptions);
        }

        public void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            _docStore.ExecuteIndex(indexCreationTask);
        }

        public Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            return _docStore.ExecuteIndexAsync(indexCreationTask);
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexCreationTasks)
        {
            _docStore.ExecuteIndexes(indexCreationTasks);
        }

        public Task ExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks)
        {
            return _docStore.ExecuteIndexesAsync(indexCreationTasks);
        }

        public BulkInsertOperation BulkInsert(string database = null, CancellationToken token = default)
        {
            return _docStore.BulkInsert(database, token);
        }

        public string Identifier
        {
            get { return _docStore.Identifier; }
            set { _docStore.Identifier = value; }
        }

        public DocumentConventions Conventions => _docStore.Conventions;

        public string[] Urls => _docStore.Urls;

        public void Delete<T>(T entity)
        {
            _lazySession.Value.Delete(entity);
        }

        public void Delete(string id)
        {
            _lazySession.Value.Delete(id);
        }

        public ILoaderWithInclude<object> Include(string path)
        {
            return _lazySession.Value.Include(path);
        }

        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, string>> path)
        {
            return _lazySession.Value.Include(path);
        }

        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, string>> path)
        {
            return _lazySession.Value.Include<T, TInclude>(path);
        }

        public T Load<T>(string id)
        {
            return _lazySession.Value.Load<T>(id);
        }

        public Dictionary<string, T> Load<T>(IEnumerable<string> ids, Action<IIncludeBuilder<T>> includes)
        {
            return _lazySession.Value.Load<T>(ids, includes);
        }

        public IRavenQueryable<T> Query<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            return _lazySession.Value.Query<T>(indexName, collectionName, isMapReduce);
        }

        public IRavenQueryable<T> Query<T>()
        {
            return _lazySession.Value.Query<T>();
        }

        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return _lazySession.Value.Query<T, TIndexCreator>();
        }

        public void SaveChanges()
        {
            _lazySession.Value.SaveChanges();
        }

        public void Store(object entity, string id)
        {
            _lazySession.Value.Store(entity, id);
        }

        public void Store(object entity, string changeVector, string id)
        {
            _lazySession.Value.Store(entity, changeVector, id);
        }

        public void Store(object entity)
        {
            _lazySession.Value.Store(entity);
        }

        public ISessionDocumentCounters CountersFor(string documentId)
        {
            return _lazySession.Value.CountersFor(documentId);
        }

        public ISessionDocumentCounters CountersFor(object entity)
        {
            return _lazySession.Value.CountersFor(entity);
        }

        public void Delete(string id, string expectedChangeVector)
        {
            _lazySession.Value.Delete(id, expectedChangeVector);
        }

        ILoaderWithInclude<object> IDocumentSession.Include(string path)
        {
            return _lazySession.Value.Include(path);
        }

        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return _lazySession.Value.Include(path);
        }

        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return _lazySession.Value.Include(path);
        }

        Dictionary<string, T> IDocumentSession.Load<T>(IEnumerable<string> ids)
        {
            return _lazySession.Value.Load<T>(ids);
        }

        public T Load<T>(string id, Action<IIncludeBuilder<T>> includes)
        {
            return _lazySession.Value.Load<T>(id, includes);
        }

        public IDatabaseChanges Changes(string database, string nodeTag)
        {
            return _docStore.Changes(database, nodeTag);
        }

        public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, string database = null)
        {
            return _docStore.AggressivelyCacheFor(cacheDuration, database);
        }

        public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, AggressiveCacheMode mode, string database = null)
        {
            return _docStore.AggressivelyCacheFor(cacheDuration, mode, database);
        }

        public IDisposable AggressivelyCache(string database = null)
        {
            return _docStore.AggressivelyCache(database);
        }

        public IDisposable DisableAggressiveCaching(string database = null)
        {
            return _docStore.DisableAggressiveCaching(database);
        }

        public void ExecuteIndex(AbstractIndexCreationTask task, string database = null)
        {
            _docStore.ExecuteIndex(task, database);
        }

        public void ExecuteIndexes(IEnumerable<AbstractIndexCreationTask> tasks, string database = null)
        {
            _docStore.ExecuteIndexes(tasks, database);
        }

        public Task ExecuteIndexAsync(AbstractIndexCreationTask task, string database = null, CancellationToken token = default)
        {
            return _docStore.ExecuteIndexAsync(task, database, token);
        }

        public Task ExecuteIndexesAsync(IEnumerable<AbstractIndexCreationTask> tasks, string database = null, CancellationToken token = default)
        {
            return _docStore.ExecuteIndexesAsync(tasks, database, token);
        }

        public RequestExecutor GetRequestExecutor(string database = null)
        {
            return _docStore.GetRequestExecutor(database);
        }

        public IDisposable SetRequestTimeout(TimeSpan timeout, string database = null)
        {
            return _docStore.SetRequestTimeout(timeout, database);
        }

        public IAdvancedSessionOperations Advanced
        {
            get { return _lazySession.Value.Advanced; }
        }

        IAdvancedSessionOperations IDocumentSession.Advanced => _lazySession.Value.Advanced;

        public X509Certificate2 Certificate => _docStore.Certificate;

        DocumentConventions IDocumentStore.Conventions => _docStore.Conventions;

        DocumentSubscriptions IDocumentStore.Subscriptions => _docStore.Subscriptions;

        public string Database { get => _docStore.Database; set => _docStore.Database = value; }

        public MaintenanceOperationExecutor Maintenance => _docStore.Maintenance;

        public OperationExecutor Operations => _docStore.Operations;

        public DatabaseSmuggler Smuggler => _docStore.Smuggler;
    }
}
