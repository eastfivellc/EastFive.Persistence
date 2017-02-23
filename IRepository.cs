using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace EastFive.Persistence
{
    public delegate Task<TResult> RetryDelegateAsync<TResult>(
               Func<TResult> retry,
               Func<int, TResult> timeout);

    public interface IRepository
    {
        Task<TResult> UpdateAsync<TData, TResult>(Guid id,
            Func<TData, Func<Task>, Task<TResult>> onUpdate,
            Func<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeoutAsync = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity;
    }
}
