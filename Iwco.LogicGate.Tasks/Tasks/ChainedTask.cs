using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Iwco.LogicGate.Tasks.Tasks
{
    public class ChainedTask : TaskBaseTask<ChainedTaskOptions>
    {
        private readonly LogicGateSyncTask _lgTask;
        private readonly VendorMasterSyncTask _vmTask;

        public ChainedTask(
            ILogger<ChainedTask> logger,
            ChainedTaskOptions options,
            LogicGateSyncTask lgTask,
            VendorMasterSyncTask vmTask
        ) : base(logger, options)
        {
            _lgTask = lgTask;
            _vmTask = vmTask;
        }

        /// <summary>
        /// This is where we chain the tasks:
        /// 1) Run LogicGateSyncTask
        /// 2) Only run VendorMasterSyncTask if LogicGateSyncTask succeeds
        /// </summary>
        public override async Task<(bool IsSuccess, bool IsComplete)> OnExecuteStep(CancellationToken stoppingToken)
        {
            TaskLogger.LogInformation("ChainedTask => Starting LogicGateSyncTask...");
            var (lgSuccess, lgComplete) = await _lgTask.OnExecuteStep(stoppingToken);
            if (!lgSuccess || !lgComplete)
            {
                TaskLogger.LogError("LogicGateSyncTask ended unsuccessfully. Aborting chain.");
                return (false, true); 
            }

            TaskLogger.LogInformation("ChainedTask => Starting VendorMasterSyncTask...");
            var (vmSuccess, vmComplete) = await _vmTask.OnExecuteStep(stoppingToken);
            if (!vmSuccess || !vmComplete)
            {
                TaskLogger.LogError("VendorMasterSyncTask ended unsuccessfully.");
                return (false, true);
            }

            TaskLogger.LogInformation("ChainedTask => All tasks completed successfully!");
            return (true, true);
        }
    }
}
