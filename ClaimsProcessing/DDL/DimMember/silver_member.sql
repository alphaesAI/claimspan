CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_member (
 -- Operational & Ingestion Metadata (Kept business naming)
  esaiInternalPersonID string
, uniqueRecord string 
, clientID string
, fileID int
, loadDateTime date
, fileLayoutID int
, fileLayoutDescription string
, pmup string
, isCurrentPMUP int
, hashKey string

-- Exact FHIR Patient/Coverage Top-Level Fields
, id string                                      -- Patient.id
, active boolean                                 -- Patient.active
, gender string                                  -- Patient.gender
, birthDate date                                 -- Patient.birthDate
, deceasedDate date                              -- Patient.deceasedDate (or deceasedDateTime)

-- Patient.identifier array 
-- Consolidates: UniquePersonKey, PlanMemberID, EnrolleeUniqueID, MedicaidID, MaskedMemberID, and AlternateKeys 1-10
, identifier ARRAY<STRUCT<
    use: string,
    system: string,
    value: string
  >>

-- Patient.name array
-- Consolidates: FirstName, LastName, MiddleInitial
, name ARRAY<STRUCT<
    use: string,
    text: string,
    family: string,
    given: ARRAY<string>
  >>

-- Patient.address array
-- Consolidates: Permanent (home) and Mailing (billing) lines, cities, states, zip codes, and counties
, address ARRAY<STRUCT<
    use: string,
    type: string,
    text: string,
    line: ARRAY<string>,
    city: string,
    district: string,                            -- FHIR uses district for County
    state: string,
    postalCode: string
  >>

-- Patient.telecom array
-- Consolidates: PhoneNumber, Email, Fax
, telecom ARRAY<STRUCT<
    system: string,
    value: string,
    use: string
  >>

-- Patient.contact array
-- Consolidates: CaretakerFirstName, CaretakerLastName, CaretakerMiddleInitial
, contact ARRAY<STRUCT<
    relationship: ARRAY<STRUCT<coding: ARRAY<STRUCT<system: string, code: string, display: string>>, text: string>>,
    name: STRUCT<use: string, family: string, given: ARRAY<string>>
  >>

-- Patient.communication array
-- Consolidates: SpokenLanguage, WrittenLanguage, OtherLanguage, and source codes
, communication ARRAY<STRUCT<
    language: STRUCT<
      coding: ARRAY<STRUCT<system: string, code: string, display: string>>,
      text: string
    >,
    preferred: boolean
  >>

-- Patient.extension array
-- Consolidates: RaceCode, EthnicityCode, USCitizen, EnrolleeEducation, EnrolleeEmployment
, extension ARRAY<STRUCT<
    url: string,
    valueCode: string,
    valueString: string,
    extension: ARRAY<STRUCT<url: string, valueCoding: STRUCT<system: string, code: string, display: string>, valueString: string>>
  >>

-- Coverage Resource fields
, subscriberId string                             -- Coverage.subscriberId
, beneficiary string                             -- Coverage.beneficiary Reference ID

-- Product context mapped via a dynamic type
, coverageProduct STRUCT<
    id: string,
    type: string
  >
) USING delta;
