namespace Iwco.LogicGate.Models.Records
{
    public class LogicGateSupplierFields
    {
        public string ErpMasterRollupName { get; set; }
        public string SupplierName { get; set; }
        public string ErpGroupId { get; set; }
        public string ErpGroupDescription { get; set; }
        public string PreferredPaymentType { get; set; }
        public string FinancialTier { get; set; }
        public string RenegotiatedPaymentTerms { get; set; }
        public string ErpSetup { get; set; }
        public string MonarchSupplierNumber1 { get; set; }
        public string MonarchSupplierNumber2 { get; set; }
        public string MonarchSupplierNumber3 { get; set; }
        public string MonarchSupplierNumber4 { get; set; }
        public bool IsActive { get; set; } // 🆕 Add this here
    }
}
