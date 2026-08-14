"""
Comprehensive claims mapping for EDI 837
Provides the raw source fields needed to derive the silver_gapsincare table
"""

MAPPINGS = {
    "name": "Claims Comprehensive Schema Mapping",
    "mapping_type": "only_mapped",
    
    "expressions": {
        # ============================================================
        # CORE PATIENT IDENTIFIERS (Links to Member Dimension)
        # ============================================================
        "MEMBER_ID": "detail.transaction_set_header_loop.claim_loop[0].subscriber_loop.member_NM1.insured_id",
        "PATIENT_LAST_NAME": "detail.transaction_set_header_loop.claim_loop[0].subscriber_loop.member_NM1.insured_last_name",
        "PATIENT_FIRST_NAME": "detail.transaction_set_header_loop.claim_loop[0].subscriber_loop.member_NM1.insured_first_name",
        
        # ============================================================
        # MEDICAL CODES (The HEDIS Filter Keys)
        # ============================================================
        # Primary procedure/billing code used to evaluate HEDIS compliance 83036
        "MEDICAL_CODE": "detail.transaction_set_header_loop.claim_loop[0].service_line_loop[0].professional_service_SV1.procedure_code",
        "PROCEDURE_MODIFIER_1": "detail.transaction_set_header_loop.claim_loop[0].service_line_loop[0].professional_service_SV1.procedure_modifier_01",
        
        # Diagnosis Codes (ICD-10 pointers for chronic condition verification)
        "PRINCIPAL_DIAGNOSIS_CODE": "detail.transaction_set_header_loop.claim_loop[0].health_care_information_HI.diagnosis_code_01",
        "DIAGNOSIS_CODE_2": "detail.transaction_set_header_loop.claim_loop[0].health_care_information_HI.diagnosis_code_02",
        
        # ============================================================
        # SERVICE DATES & QUANTITIES (For Timeline Validation & PDC Math)
        # ============================================================
        # The exact date of medical service (used to build 'serviceNeededByDate')
        "SERVICE_DATE": "detail.transaction_set_header_loop.claim_loop[0].service_line_loop[0].service_date_DTP.date_value",
        
        # Days supply (Required for Statin Adherence / PDC math placeholders)
        # "DAYS_SUPPLY": "detail.transaction_set_header_loop.claim_loop[0].service_line_loop[0].professional_service_SV1.quantity",
        
        # ============================================================
        # OPERATIONAL CLAIMS METADATA
        # ============================================================
        "CLAIM_CONTROL_NUMBER": "detail.transaction_set_header_loop.claim_loop[0].claim_submission_CLM.claim_submitter_identifier",
        "TOTAL_CLAIM_CHARGE": "detail.transaction_set_header_loop.claim_loop[0].claim_submission_CLM.total_claimed_charge_amount",
        "FACILITY_TYPE_CODE": "detail.transaction_set_header_loop.claim_loop[0].claim_submission_CLM.facility_type_code",
    }
}