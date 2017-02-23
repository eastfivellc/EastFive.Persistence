using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using BlackBarLabs.Extensions;
using BlackBarLabs.Linq;
using BlackBarLabs;

namespace EastFive.Persistence
{
    public partial class Transaction<TFailure>
    {
        public class Result
        {
            public TFailure FailureResult { get; private set; }
            

            public bool IsFailure { get; private set; }

            private Func<Task> rollback = default(Func<Task>);

            private Result()
            {

            }

            internal static Result Success(Func<Task> rollback)
            {
                return new Result()
                {
                    rollback = rollback,
                    IsFailure = false,
                };
            }

            internal static Result Failure(TFailure failureResult)
            {
                return new Result()
                {
                    rollback = default(Func<Task>),
                    FailureResult = failureResult,
                    IsFailure = true,
                };
            }

            internal Task Rollback()
            {
                if (default(Func<Task>) != rollback)
                    return rollback();
                return true.ToTask();
            }
        }

        public delegate Task<Result> TransactionTaskDelegate(
            Func<Func<Task>, Result> success,
            Func<TFailure, Result> failure);

        protected IRepository repo;

        public Transaction(IRepository repo)
        {
            this.repo = repo;
            this.Tasks = new TransactionTaskDelegate[] { };
        }

        public TransactionTaskDelegate [] Tasks;

        public void AddTask(TransactionTaskDelegate task)
        {
            this.Tasks = this.Tasks.Append(task).ToArray();
        }

        public async Task<TResult> ExecuteAsync<TResult>(
            Func<TResult> success,
            Func<TFailure, TResult> failed)
        {
            var results = await Tasks
                .Select(async task =>
                {
                    var rollbackResult = await task.Invoke(
                        (rollback) => Result.Success(rollback),
                        (failureResultReturned) => Result.Failure(failureResultReturned));
                    return rollbackResult;
                })
                .WhenAllAsync();

            var resultGlobal = await results.FirstOrDefault(
                result => result.IsFailure,
                async (failedResult) =>
                {
                    await results.Select(result => result.Rollback()).WhenAllAsync();
                    return failed(failedResult.FailureResult);
                },
                async () =>
                {
                    return await success().ToTask();
                });
            return resultGlobal;
        }
    }
    
}
