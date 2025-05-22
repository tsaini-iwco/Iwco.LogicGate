using System;
using System.IO;
using System.Linq;

namespace Iwco.LogicGate.Tasks.Services
{
    public static class FinanceMappingFileLocator
    {
        private const string PATTERN = "*.xlsx";

        public static string TryGetFile(out bool isInReadFolder)
        {
            var baseDir = Environment.GetEnvironmentVariable("BASE_FOLDER") ?? @"E:\Data";
            var landing = Path.Combine(baseDir, "LANDING", "EXCEL", "FinanceMapping");
            var readDir = Path.Combine(landing, "READY");
            var procDir = Path.Combine(landing, "PROCESSED");

            Directory.CreateDirectory(readDir);
            Directory.CreateDirectory(procDir);

            // 1) Look for a fresh file in READY
            var readFile = Directory
                .EnumerateFiles(readDir, PATTERN, SearchOption.TopDirectoryOnly)
                .FirstOrDefault();

            if (readFile != null)
            {
                isInReadFolder = true;
                return readFile;          // leave it in READY for now
            }

            // 2) Fallback to the newest file in PROCESSED
            var newest = Directory
                .EnumerateDirectories(procDir)                    
                .OrderByDescending(d => d)                        
                .SelectMany(d => Directory.EnumerateFiles(d, PATTERN))
                .OrderByDescending(File.GetLastWriteTimeUtc)
                .FirstOrDefault();

            if (newest == null)
                throw new FileNotFoundException("No FinanceMapping workbook found.");

            isInReadFolder = false;
            return newest;
        }

        
        public static void ArchiveReadFile(string readFileFullPath)
        {
            var dir = Path.GetDirectoryName(readFileFullPath)!;

            // Only move files that actually came from the READY folder.
            if (!dir.EndsWith(Path.Combine("FinanceMapping", "READY"), StringComparison.OrdinalIgnoreCase))
                return;

            
            var financeMappingDir = Directory.GetParent(dir)!.FullName; 
            var datedFolder = DateTime.UtcNow.ToString("yyyy-MM-dd");
            var procDir = Path.Combine(financeMappingDir, "PROCESSED", datedFolder);

            Directory.CreateDirectory(procDir);

            var dest = Path.Combine(procDir, Path.GetFileName(readFileFullPath));
            File.Move(readFileFullPath, dest, overwrite: true);
        }
    }
}
