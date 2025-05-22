USE [DataLake]  -- Replace with the actual database name
GO

-------------------------------------------------------------------
-- 1) Drop the VendorMaster table (if it exists)
-------------------------------------------------------------------
IF EXISTS (
    SELECT * 
    FROM sys.objects 
    WHERE object_id = OBJECT_ID(N'[dbo].[VendorMaster]') 
        AND type in (N'U')
)
BEGIN
    DROP TABLE [dbo].[VendorMaster];
END
GO

-------------------------------------------------------------------
-- 2) Drop the VendorMaster_Revision sequence (if it exists)
-------------------------------------------------------------------
IF EXISTS (
    SELECT * 
    FROM sys.sequences 
    WHERE object_id = OBJECT_ID(N'[dbo].[VendorMaster_Revision]')
)
BEGIN
    DROP SEQUENCE [dbo].[VendorMaster_Revision];
END
GO

-------------------------------------------------------------------
-- 3) Create the VendorMaster_Revision sequence
-------------------------------------------------------------------
CREATE SEQUENCE [dbo].[VendorMaster_Revision]
    AS BIGINT
    START WITH 1
    INCREMENT BY 1;
GO

-------------------------------------------------------------------
-- 4) Drop MergeVendorMasterTableType (if it exists)
-------------------------------------------------------------------
IF TYPE_ID(N'MergeVendorMasterTableType') IS NOT NULL
    DROP TYPE [dbo].[MergeVendorMasterTableType];
GO

-------------------------------------------------------------------
-- 5) Create MergeVendorMasterTableType for batch processing
-------------------------------------------------------------------
CREATE TYPE [dbo].[MergeVendorMasterTableType] AS TABLE
(
    VendorMasterId BIGINT NULL,               -- Will be NULL for new inserts
    SupplierName VARCHAR(255) NOT NULL,       -- Master Rollup Name
    VendorStatus VARCHAR(50) NOT NULL,        -- Active/Inactive
    StatusChangedDate DATETIME2 NULL,         -- Last status change date

    IsDeleted BIT NOT NULL DEFAULT 0,         -- Soft delete flag
    IsActive BIT NOT NULL,                    -- True/False indicating active status

    SupplierDetails NVARCHAR(MAX) NULL,       -- JSON of system-specific supplier details

    SourceSystem VARCHAR(50) NOT NULL,        -- Who won: Monarch or LogicGate
    VersionNo INT NOT NULL DEFAULT 1,         -- Entity-level version tracking
    ChangedDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),   -- Last modification timestamp
    ChangedBy VARCHAR(255) NOT NULL,          -- Process/User who made the change

    Revision BIGINT NOT NULL                  -- Assigned in stored procedure
);
GO

-------------------------------------------------------------------
-- 6) Create the VendorMaster table
-------------------------------------------------------------------
CREATE TABLE [dbo].[VendorMaster]
(
    VendorMasterId BIGINT PRIMARY KEY IDENTITY(1,1) NOT NULL,

    SupplierName VARCHAR(255) NOT NULL UNIQUE,   -- Master Rollup Name
    VendorStatus VARCHAR(50) NOT NULL,           -- Active/Inactive
    StatusChangedDate DATETIME2 NULL,            -- Last status change date
    
    IsDeleted BIT NOT NULL DEFAULT 0,            -- Soft delete flag
    IsActive BIT NOT NULL,                       -- True/False indicating active status
    
    SupplierDetails NVARCHAR(MAX) NULL,          -- JSON array of system-specific supplier names/details
    
    SourceSystem VARCHAR(50) NOT NULL,           -- Who won: Monarch or LogicGate
    VersionNo INT NOT NULL DEFAULT 1,            -- Entity-level version tracking
    ChangedDate DATETIME2 NOT NULL DEFAULT SYSDATETIME(),   -- Last modification timestamp
    ChangedBy VARCHAR(255) NOT NULL,             -- Process/User who made the change
    
    Revision BIGINT NOT NULL DEFAULT (NEXT VALUE FOR [dbo].[VendorMaster_Revision]) 
        -- Global change tracking
);
GO

-------------------------------------------------------------------
-- 7) Drop the stored procedure if it exists
-------------------------------------------------------------------
IF EXISTS (
    SELECT * 
    FROM sys.objects 
    WHERE object_id = OBJECT_ID(N'[dbo].[p_MergeVendorMasterByTable]') 
        AND type in (N'P')
)
BEGIN
    DROP PROCEDURE [dbo].[p_MergeVendorMasterByTable];
END
GO

-------------------------------------------------------------------
-- 8) Create the stored procedure p_MergeVendorMasterByTable
-------------------------------------------------------------------
CREATE PROCEDURE [dbo].[p_MergeVendorMasterByTable]
    @VendorMasterTable [dbo].[MergeVendorMasterTableType] READONLY
AS
BEGIN
    SET NOCOUNT ON;
    -- DEBUG: Create temp table to capture changes
IF OBJECT_ID('tempdb..#DebugUpdates') IS NOT NULL
    DROP TABLE #DebugUpdates;

CREATE TABLE #DebugUpdates (
    SupplierName VARCHAR(255),
    FieldChanged VARCHAR(50),
    OldValue NVARCHAR(MAX),
    NewValue NVARCHAR(MAX)
);


    -- Get the new batch revision value
    DECLARE @currentRevision BIGINT = NEXT VALUE FOR dbo.VendorMaster_Revision;
    DECLARE @currentDateTime DATETIME2 = SYSDATETIME();

    -- Merge Logic
    MERGE INTO [dbo].[VendorMaster] AS Target
    USING @VendorMasterTable AS Source
        ON Target.SupplierName = Source.SupplierName
    
    -- 🟢 Insert new records
    WHEN NOT MATCHED BY Target THEN
        INSERT (
            SupplierName, 
            VendorStatus, 
            StatusChangedDate, 
            IsDeleted, 
            IsActive,
            SupplierDetails, 
            SourceSystem, 
            VersionNo, 
            ChangedDate, 
            ChangedBy, 
            Revision
        )
        VALUES (
            Source.SupplierName, 
            Source.VendorStatus, 
            Source.StatusChangedDate, 
            Source.IsDeleted, 
            Source.IsActive,
            Source.SupplierDetails, 
            Source.SourceSystem, 
            1,                    -- Version starts at 1 for new records
            @currentDateTime, 
            Source.ChangedBy, 
            @currentRevision      -- Assign new batch revision
        )

    -- 🟢 Update only if something actually changed
    WHEN MATCHED 
AND (
    ISNULL(Target.VendorStatus, '') <> ISNULL(Source.VendorStatus, '')
    OR ISNULL(Target.StatusChangedDate, '1900-01-01') <> ISNULL(Source.StatusChangedDate, '1900-01-01')
    OR ISNULL(Target.IsDeleted, 0) <> ISNULL(Source.IsDeleted, 0)
    OR ISNULL(Target.IsActive, 0) <> ISNULL(Source.IsActive, 0)
    OR ISNULL(Target.SupplierDetails, '') <> ISNULL(Source.SupplierDetails, '')
    OR ISNULL(Target.SourceSystem, '') <> ISNULL(Source.SourceSystem, '')
    OR ISNULL(Target.ChangedBy, '') <> ISNULL(Source.ChangedBy, '')
)

    THEN
       UPDATE SET
    Target.VersionNo = Target.VersionNo + 1,
    Target.ChangedDate = @currentDateTime,
    Target.ChangedBy = Source.ChangedBy,
    Target.Revision = @currentRevision
OUTPUT 
    INSERTED.SupplierName,
    CASE 
        WHEN DELETED.VendorStatus <> INSERTED.VendorStatus THEN 'VendorStatus'
        WHEN DELETED.StatusChangedDate <> INSERTED.StatusChangedDate THEN 'StatusChangedDate'
        WHEN DELETED.IsDeleted <> INSERTED.IsDeleted THEN 'IsDeleted'
        WHEN DELETED.IsActive <> INSERTED.IsActive THEN 'IsActive'
        WHEN DELETED.SupplierDetails <> INSERTED.SupplierDetails THEN 'SupplierDetails'
        WHEN DELETED.SourceSystem <> INSERTED.SourceSystem THEN 'SourceSystem'
        WHEN DELETED.ChangedBy <> INSERTED.ChangedBy THEN 'ChangedBy'
    END AS FieldChanged,
    CASE 
        WHEN DELETED.VendorStatus <> INSERTED.VendorStatus THEN DELETED.VendorStatus
        WHEN DELETED.StatusChangedDate <> INSERTED.StatusChangedDate THEN CONVERT(NVARCHAR, DELETED.StatusChangedDate, 120)
        WHEN DELETED.IsDeleted <> INSERTED.IsDeleted THEN CONVERT(NVARCHAR, DELETED.IsDeleted)
        WHEN DELETED.IsActive <> INSERTED.IsActive THEN CONVERT(NVARCHAR, DELETED.IsActive)
        WHEN DELETED.SupplierDetails <> INSERTED.SupplierDetails THEN DELETED.SupplierDetails
        WHEN DELETED.SourceSystem <> INSERTED.SourceSystem THEN DELETED.SourceSystem
        WHEN DELETED.ChangedBy <> INSERTED.ChangedBy THEN DELETED.ChangedBy
    END AS OldValue,
    CASE 
        WHEN DELETED.VendorStatus <> INSERTED.VendorStatus THEN INSERTED.VendorStatus
        WHEN DELETED.StatusChangedDate <> INSERTED.StatusChangedDate THEN CONVERT(NVARCHAR, INSERTED.StatusChangedDate, 120)
        WHEN DELETED.IsDeleted <> INSERTED.IsDeleted THEN CONVERT(NVARCHAR, INSERTED.IsDeleted)
        WHEN DELETED.IsActive <> INSERTED.IsActive THEN CONVERT(NVARCHAR, INSERTED.IsActive)
        WHEN DELETED.SupplierDetails <> INSERTED.SupplierDetails THEN INSERTED.SupplierDetails
        WHEN DELETED.SourceSystem <> INSERTED.SourceSystem THEN INSERTED.SourceSystem
        WHEN DELETED.ChangedBy <> INSERTED.ChangedBy THEN INSERTED.ChangedBy
    END AS NewValue
INTO #DebugUpdates;

-- Return all updates made (debug view only)
SELECT * FROM #DebugUpdates;

END

GO
