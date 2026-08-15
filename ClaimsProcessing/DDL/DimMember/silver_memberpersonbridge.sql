CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_memberpersonbridge (
 -- Operational & Ingestion Metadata (Kept clear business naming)
esaiInternalPersonID string                   -- Internal person identifier bridge
, isCurrent int
, uniqueRecord string
, fileLayoutID string
, fileId bigint
, hashKey string
, isCurrentPlanMemberID int
, isCurrentUniquePersonKey int
, isOriginalMemberID int
, pmup string
, isCurrentPMUP int

-- Exact FHIR Patient/Coverage Core Fields (camelCase, native arrays/structs)
, id string                                      -- Patient.id
, gender string                                  -- Patient.gender
, birthDate string                               -- Patient.birthDate (Kept as string for Silver staging)

-- Patient.identifier array for PlanMemberID, BeneficiaryID, and UniquePersonKey
, identifier ARRAY<STRUCT<
    use: string,
    system: string,
    value: string
  >>

-- Patient.name array (Replaces FirstName and LastName)
, name ARRAY<STRUCT<
    use: string,
    text: string,
    family: string,
    given: ARRAY<string>
  >>

-- Patient.address array (Replaces PermanentAddressLine1)
, address ARRAY<STRUCT<
    use: string,
    line: ARRAY<string>,
    city: string,
    state: string,
    postalCode: string
  >>

-- Patient.telecom array (Replaces PhoneNumber)
, telecom ARRAY<STRUCT<
    system: string,
    value: string,
    use: string
  >>
) USING delta;
