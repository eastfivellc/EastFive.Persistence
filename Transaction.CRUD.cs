using BlackBarLabs.Extensions;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace EastFive.Persistence
{
    public partial class Transaction<TFailure>
    {
        public void AddTaskUpdate<TDocument>(
            Guid docId,
            Func<TDocument, bool> mutateUpdate,
            Func<TDocument, bool> mutateRollback,
            Func<TFailure> onNotFound)
            where TDocument : class, ITableEntity
        {
            this.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Result>(docId,
                        async (doc, saveAsync) =>
                        {
                            if (!mutateUpdate(doc))
                                return success(() => true.ToTask());
                            await saveAsync();
                            return success(
                                async () =>
                                {
                                    await repo.UpdateAsync<TDocument, bool>(docId,
                                        async (docRollback, saveRollbackAsyc) =>
                                        {
                                            if(mutateRollback(docRollback));
                                                await saveRollbackAsyc();
                                            return true;
                                        },
                                        () => false);
                                });
                        },
                        () => failure(onNotFound()));
                    return r;
                });
        }
        
        public void AddTaskUpdate<TDocument>(
            Guid docId,
            Func<TDocument, bool> mutateUpdate,
            Func<TDocument, bool> mutateRollback,
            Func<TFailure> onMutateFailed,
            Func<TFailure> onNotFound)
            where TDocument : class, ITableEntity
        {
            this.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Result>(docId,
                        async (doc, saveAsync) =>
                        {
                            if (!mutateUpdate(doc))
                                return failure(onMutateFailed());
                            await saveAsync();
                            return success(
                                async () =>
                                {
                                    await repo.UpdateAsync<TDocument, bool>(docId,
                                        async (docRollback, saveRollbackAsyc) =>
                                        {
                                            if (mutateRollback(docRollback)) ;
                                            await saveRollbackAsyc();
                                            return true;
                                        },
                                        () => false);
                                });
                        },
                        () => failure(onNotFound()));
                    return r;
                });
        }

        public struct Carry<T>
        {
            public T carry;
        }

        public void AddTaskUpdate<TDocument, TCarry>(
            Guid docId,
            Func<TDocument, Carry<TCarry>?> mutateUpdate,
            Func<TDocument, TCarry, bool> mutateRollback,
            Func<TFailure> onMutateFailed,
            Func<TFailure> onNotFound)
            where TDocument : class, ITableEntity
        {
            this.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Result>(docId,
                        async (doc, saveAsync) =>
                        {
                            var updateResult = mutateUpdate(doc);
                            if (!updateResult.HasValue)
                                return failure(onMutateFailed());
                            await saveAsync();
                            return success(
                                async () =>
                                {
                                    await repo.UpdateAsync<TDocument, bool>(docId,
                                        async (docRollback, saveRollbackAsyc) =>
                                        {
                                            if (mutateRollback(docRollback)) ;
                                            await saveRollbackAsyc();
                                            return true;
                                        },
                                        () => false);
                                });
                        },
                        () => failure(onNotFound()));
                    return r;
                });
        }

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, T> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Carry<T>?>(docId,
                        async (doc, save) =>
                        {
                            var carry = mutateUpdate(doc);
                            await save(doc);
                            return new Carry<T>
                            {
                                carry = carry,
                            };
                        },
                        () => default(Carry<T>?));
                    if (r.HasValue)
                        return success(
                            async () =>
                            {
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        mutateRollback(r.Value.carry, doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    return failure(onNotFound());
                });
        }

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<T, TRollback> rollback,
            Guid docId,
            Func<TDocument, T> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Carry<T>?>(docId,
                        async (doc, save) =>
                        {
                            var carry = mutateUpdate(doc);
                            await save(doc);
                            return new Carry<T>
                            {
                                carry = carry,
                            };
                        },
                        () => default(Carry<T>?));
                    if (r.HasValue)
                        return success(r.Value.carry,
                            async () =>
                            {
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        mutateRollback(r.Value.carry, doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    return failure(onNotFound());
                });
        }

        public static void AddTaskDeleteJoin<TRollback, TDocument>(this RollbackAsync<Guid?, TRollback> rollback,
            Guid docId,
            Func<TDocument, Guid?> mutateDelete,
            Action<Guid, TDocument> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate(docId,
                (TDocument doc) =>
                {
                    var joinId = mutateDelete(doc);
                    return joinId;
                },
                (joinId, doc) =>
                {
                    if (joinId.HasValue)
                    {
                        mutateRollback(joinId.Value, doc);
                        return true;
                    }
                    return false;
                },
                onNotFound,
                repo);
        }

        public static void AddTaskCheckup<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TRollback> onDoesNotExists,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    return await repo.FindByIdAsync(docId,
                        (TDocument doc) => success(() => 1.ToTask()), () => failure(onDoesNotExists()));
                });
        }

        public static void AddTaskCreate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId, TDocument document,
            Func<TRollback> onAlreadyExists,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    return await repo.CreateAsync(docId, document,
                        () => success(
                            async () =>
                            {
                                await repo.DeleteIfAsync<TDocument, bool>(docId,
                                    async (doc, delete) => { await delete(); return true; },
                                    () => false);
                            }),
                        () => failure(onAlreadyExists()));
                });
        }

        public static void AddTaskCreateOrUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, bool> isValidAndMutate,
            Func<TDocument, bool> mutateRollback,
            Func<TRollback> onFail,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                (success, failure) =>
                {
                    return repo.CreateOrUpdateAsync<TDocument, RollbackAsync<TRollback>.RollbackResult>(docId,
                        async (created, doc, save) =>
                        {
                            if (!isValidAndMutate(doc))
                                return failure(onFail());
                            
                            await save(doc);
                            return success(
                                async () =>
                                {
                                    if (created)
                                    {
                                        await repo.DeleteIfAsync<TDocument, bool>(docId,
                                            async (docDelete, delete) =>
                                            {
                                                // TODO: Check etag if(docDelete.ET)
                                                await delete();
                                                return true;
                                            },
                                            () => false);
                                        return;
                                    }
                                    await repo.UpdateAsync<TDocument, bool>(docId,
                                        async (docRollback, saveRollback) =>
                                        {
                                            if(mutateRollback(docRollback))
                                                await saveRollback(docRollback);
                                            return true;
                                        },
                                        () => false);
                                });
                        });
                });
        }
        
        public static void AddTaskAsyncCreateOrUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, Task<bool>> isValidAndMutate,
            Func<TDocument, Task<bool>> mutateRollback,
            Func<TRollback> onFail,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                (success, failure) =>
                {
                    return repo.CreateOrUpdateAsync<TDocument, RollbackAsync<TRollback>.RollbackResult>(docId,
                        async (created, doc, save) =>
                        {
                            if (!await isValidAndMutate(doc))
                                return failure(onFail());

                            await save(doc);
                            return success(
                                async () =>
                                {
                                    if (created)
                                    {
                                        await repo.DeleteIfAsync<TDocument, bool>(docId,
                                            async (docDelete, delete) =>
                                            {
                                                // TODO: Check etag if(docDelete.ET)
                                                await delete();
                                                return true;
                                            },
                                            () => false);
                                        return;
                                    }
                                    await repo.UpdateAsync<TDocument, bool>(docId,
                                        async (docRollback, saveRollback) =>
                                        {
                                            if (await mutateRollback(docRollback))
                                                await saveRollback(docRollback);
                                            return true;
                                        },
                                        () => false);
                                });
                        });
                });
        }

        public static async Task<TRollback> ExecuteAsync<TRollback>(this RollbackAsync<TRollback> rollback,
            Func<TRollback> onSuccess)
        {
            return await rollback.ExecuteAsync(onSuccess, r => r);
        }

        public static async Task<TRollback> ExecuteDeleteJoinAsync<TRollback, TDocument>(this RollbackAsync<Guid?, TRollback> rollback,
            Func<TRollback> onSuccess,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            var result = await await rollback.ExecuteAsync<Task<TRollback>>(
                async (joinIds) =>
                {
                    var joinId = joinIds.First(joinIdCandidate => joinIdCandidate.HasValue);
                    if (!joinId.HasValue)
                        return onSuccess();
                    return await repo.DeleteIfAsync<TDocument, TRollback>(joinId.Value,
                        async (doc, delete) =>
                        {
                            await delete();
                            return onSuccess();
                        },
                        () =>
                        {
                            // TODO: Log data inconsistency
                            return onSuccess();
                        });
                },
                (failureResult) => failureResult.ToTask());
            return result;
        }
    }
}
