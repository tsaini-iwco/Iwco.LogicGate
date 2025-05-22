using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Iwco.LogicGate.Tasks.Tasks
{   
    /// <summary>
    /// Base record for all task options.
    /// </summary>
    public record TaskOptions();

    /// <summary>
    /// A base class for building “tasks” that run as HostedServices.
    /// </summary>
    public class TaskBaseTask<T> where T : TaskOptions
    {
        private readonly ILogger<TaskBaseTask<T>> _logger;
        private readonly T _options;

        public TaskBaseTask(ILogger<TaskBaseTask<T>> logger, T options)
        {
            _logger = logger;
            _options = options;
        }

        public T Options => _options;
        public ILogger TaskLogger => _logger;

        // Lifecycle hooks
        public virtual void OnApplicationStarted()
            => TaskLogger.LogInformation("Application started.");

        public virtual void OnApplicationStopping()
            => TaskLogger.LogInformation("Application stopping...");

        public virtual void OnApplicationStopped()
            => TaskLogger.LogInformation("Application stopped.");

        public virtual Task OnBeforeExecute(CancellationToken stoppingToken)
        {
            TaskLogger.LogInformation("Before task {TaskName} execution", GetType().Name);
            return Task.CompletedTask;
        }

        public virtual async Task<(bool IsSuccess, bool IsComplete)> OnExecuteStep(CancellationToken stoppingToken)
        {
            
            return (true, true);
        }

        public virtual Task OnAfterExecute(bool isSuccess)
        {
            TaskLogger.LogInformation("After task {TaskName} execution. Task was {successOrFail}",
                GetType().Name, isSuccess ? "Successful" : "Failed");
            return Task.CompletedTask;
        }

        public virtual Task StartAsync(CancellationToken cancellationToken)
        {
            TaskLogger.LogInformation("Task {TaskName} is starting...", GetType().Name);
            return Task.CompletedTask;
        }

        public virtual Task StopAsync(CancellationToken cancellationToken)
        {
            TaskLogger.LogInformation("Task {TaskName} is stopping...", GetType().Name);
            return Task.CompletedTask;
        }

        /// <summary>
        /// This nested class is registered as a HostedService. It calls the 
        /// task’s lifecycle methods at the right times.
        /// </summary>
        public class TaskService : BackgroundService
        {
            private readonly TaskBaseTask<T> _task;
            private readonly IHostApplicationLifetime _host;
            private ILogger TaskLogger => _task.TaskLogger;

            public TaskService(TaskBaseTask<T> task, IHostApplicationLifetime host)
            {
                _task = task;
                _host = host;

                host.ApplicationStarted.Register(OnStarted);
                host.ApplicationStopping.Register(OnStopping);
                host.ApplicationStopped.Register(OnStopped);
            }

            protected override async Task ExecuteAsync(CancellationToken stoppingToken)
            {
                await _task.OnBeforeExecute(stoppingToken);

                bool success = true;
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var (isStepSuccess, isComplete) = await _task.OnExecuteStep(stoppingToken);
                        success = success && isStepSuccess;
                        if (isComplete) break;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Normal cancellation
                }
                catch (Exception ex)
                {
                    TaskLogger.LogError(ex, "An error occurred while executing the task.");
                    success = false;
                }

                await _task.OnAfterExecute(success);
                _host.StopApplication();
            }

            public override async Task StartAsync(CancellationToken cancellationToken)
            {
                await _task.StartAsync(cancellationToken);
                await base.StartAsync(cancellationToken);
            }

            public override async Task StopAsync(CancellationToken cancellationToken)
            {
                await base.StopAsync(cancellationToken);
                await _task.StopAsync(cancellationToken);
            }

            private void OnStarted() => _task.OnApplicationStarted();
            private void OnStopping() => _task.OnApplicationStopping();
            private void OnStopped() => _task.OnApplicationStopped();
        }
    }
}
