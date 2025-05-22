using System;
using System.Collections.Generic;

namespace Iwco.LogicGate.Models.Records
{
    public class LogicGateRecord
    {
        public string Id { get; set; }  // Record ID from lg_records
        public LogicGateDates Dates { get; set; }  // Stores created, updated dates
        public List<LogicGateField> Fields { get; set; }  // All fields from JSON
    }

    public class LogicGateDates
    {
        public long? Created { get; set; }
        public long? DueDate { get; set; }
        public long? RecordDueDate { get; set; }
        public int? DaysUntilDue { get; set; }
        public long? Updated { get; set; }
        public long? Completed { get; set; }
        public long? LastCompleted { get; set; }

        /// <summary>
        /// Converts milliseconds timestamp to DateTime (UTC)
        /// </summary>
        private static DateTime? ConvertToDateTime(long? timestamp)
        {
            return timestamp.HasValue && timestamp > 0
                ? DateTimeOffset.FromUnixTimeMilliseconds(timestamp.Value).UtcDateTime
                : (DateTime?)null;
        }

        // Expose DateTime properties for easy access
        public DateTime? CreatedDate => ConvertToDateTime(Created);
        public DateTime? DueDateConverted => ConvertToDateTime(DueDate);
        public DateTime? RecordDueDateConverted => ConvertToDateTime(RecordDueDate);
        public DateTime? UpdatedDate => ConvertToDateTime(Updated);
        public DateTime? CompletedDate => ConvertToDateTime(Completed);
        public DateTime? LastCompletedDate => ConvertToDateTime(LastCompleted);
    }
}
