CREATE TABLE IF NOT EXISTS claimsprocessing.gold.fhir_gold_dimmember (
 -- Internal Warehouse Keys (Kept business naming)
  memberKey bigint
, effectiveStartDate date
, effectiveEndDate date
, isCurrent boolean

-- Exact FHIR Patient/Coverage Top-Level Fields and Capitalization
, id string                                      -- Patient.id
, active boolean                                 -- Patient.active
, gender string                          
        -- Patient.gender
, birthDate date                                 -- Patient.birthDate

-- Patient.identifier is a native array of structs in FHIR
, identifier ARRAY<STRUCT<
    use: string,
    system: string,
    value: string
  >>

-- Patient.name is an array of HumanName structs.
-- This natively allows to later query name[0].family or name[0].given[0]
, name ARRAY<STRUCT<
    use: string,
    text: string,
    family: string,
    given: ARRAY<string>,
    prefix: ARRAY<string>,
    suffix: ARRAY<string>
  >>

-- given
-- family

-- select first name, lastname from table name
-- where id: 900
-- name
-- given
-- family
-- Patient.deceased[x] is represented here as deceasedDateTime
, deceasedDateTime string                        -- Patient.deceasedDateTime

-- Patient.address is an array of Address structs
, address ARRAY<STRUCT<
    use: string,
    type: string,
    text: string,
    line: ARRAY<string>,
    city: string,
    district: string,
    state: string,
    postalCode: string,
    country: string
  >>

-- address : text
-- fhir name with flatten schema

-- city
-- district
-- state
-- Patient.telecom is an array of ContactPoint structs
, telecom ARRAY<STRUCT<
    system: string,
    value: string,
    use: string,
    rank: int
  >>

-- Patient.contact (e.g., Caretaker) is an array of BackboneElements
, contact ARRAY<STRUCT<
    relationship: ARRAY<STRUCT<coding: ARRAY<STRUCT<system: string, code: string, display: string>>, text: string>>,
    name: STRUCT<use: string, family: string, given: ARRAY<string>>,
    telecom: ARRAY<STRUCT<system: string, value: string>>,
    address: STRUCT<line: ARRAY<string>, city: string, state: string, postalCode: string>,
    gender: string
  >>

-- Patient.communication is an array of language structures
, communication ARRAY<STRUCT<
    language: STRUCT<
      coding: ARRAY<STRUCT<system: string, code: string, display: string>>,
      text: string
    >,
    preferred: boolean
  >>

-- Patient.extension maps US Core demographics like Race/Ethnicity
, extension ARRAY<STRUCT<
    url: string,
    valueCode: string,
    valueString: string,
    extension: ARRAY<STRUCT<url: string, valueCoding: STRUCT<system: string, code: string, display: string>, valueString: string>>
  >>

-- Coverage Resource fields (from your plan/subscriber columns)
, subscriberId string                             -- Coverage.subscriberId
, beneficiary string                             -- Coverage.beneficiary Reference ID

-- Product context mapped via a dynamic type
, coverageProduct STRUCT<
    id: string,
    type: string
  >
) USING delta;


SELECT * FROM claimsprocessing.gold.fhir_gold_dimmember;
