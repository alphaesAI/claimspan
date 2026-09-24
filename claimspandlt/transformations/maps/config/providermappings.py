"""
Provider dimension mapping for EDI 274 (Health Care Provider Directory/Hierarchy).
Maps pyedi StructuredFormatter output for 274 transaction sets to flat provider
hierarchy records matching the providerhierarchyschema.json column definitions.

274 JSON structure uses `interchange_control_header_*` naming for segments inside
`detail.interchange_control_header_loop` (complex files with HL hierarchy) or
`detail.provider_NM1_loop` (simpler files). The mapping expressions use the JSONata
`**` descendant operator to find segment arrays at any depth under detail/heading,
so both loop wrappers are handled without conditional logic.

N3/N4/PER arrays are aligned with the HL/NM1 hierarchy order:
  [0] = Tier 4 (entity_identifier_code='85', billing provider/payer)
  [1] = Tier 2 (entity_identifier_code='1P', entity_type_qualifier='2', organization)
  [2] = Location (entity_identifier_code='1P', entity_type_qualifier='1', individual provider)
"""

PROVIDERMAPPINGS = {
    "name": "Provider Dimension Schema Mapping - EDI 274",
    "mapping_type": "only_mapped",
    "expressions": {
        # =====================================================================
        # TEMPLATE
        # =====================================================================
        "TEMPLATE": " 'TEMPLATE' ",

        # =====================================================================
        # 1. PROVIDER HIERARCHY LAYOUT FIELDS (UPPERCASE KEYS)
        # =====================================================================
        "PROVIDERID":       "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='1'].identifier",
        "PROVIDERLASTNAME": "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='1'].name",
        "PROVIDERNPI":      "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='1'].identifier",

        "LOCATIONGROUPID":  "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='2'].identifier",
        "LOCATIONRANKING":  "1",
        "LOCATIONIDTYPE":   "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='1'].entity_identifier_code",
        "LOCATIONID":       "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='1'].identifier",
        "LOCATIONDESC":     "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='1'].($join([prefix, first_name, middle_name, name][$ != null], ' '))",
        "LOCATIONTIN":      "**.interchange_control_header_REF.employer_id[0]",

        "LOCATIONADDRESS1": "detail.**.interchange_control_header_N3[2].address_line_1",
        "LOCATIONADDRESS2": "detail.**.interchange_control_header_N3[2].address_line_2",
        "LOCATIONCITY":     "detail.**.interchange_control_header_N4[2].city",
        "LOCATIONSTATE":    "detail.**.interchange_control_header_N4[2].state",
        "LOCATIONZIP":      "detail.**.interchange_control_header_N4[2].zip_code",

        "COUNTYCODE":       "detail.**.interchange_control_header_N4[2].n406_406",
        "PHONENUMBER":      "detail.**.interchange_control_header_PER[1].per04_04",
        "FAXNUMBER":        "detail.**.interchange_control_header_PER[1].per06_06",
        "CONTACTPERSON":    "detail.**.interchange_control_header_PER[1].per02_02",
        "DONOTCHASE":       "detail.unmapped",

        "TIER2IDTYPE":      "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='2'].id_qualifier",
        "TIER2ID":          "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='2'].identifier",
        "TIER2DESC":        "detail.**.interchange_control_header_NM1[entity_identifier_code='1P' and entity_type_qualifier='2'].name",
        "TIER2ADDRESS1":    "detail.**.interchange_control_header_N3[1].address_line_1",
        "TIER2ADDRESS2":    "detail.**.interchange_control_header_N3[1].address_line_2",
        "TIER2CITY":        "detail.**.interchange_control_header_N4[1].city",
        "TIER2STATE":       "detail.**.interchange_control_header_N4[1].state",
        "TIER2ZIP":         "detail.**.interchange_control_header_N4[1].zip_code",

        "TIER3IDTYPE":      "detail.unmapped",
        "TIER3ID":          "detail.unmapped",
        "TIER3DESC":        "detail.unmapped",
        "TIER3ADDRESS1":    "detail.unmapped",
        "TIER3ADDRESS2":    "detail.unmapped",
        "TIER3CITY":        "detail.unmapped",
        "TIER3STATE":       "detail.unmapped",
        "TIER3ZIP":         "detail.unmapped",

        "TIER4IDTYPE":      "detail.**.interchange_control_header_NM1[entity_identifier_code='85'].billing_provider_id_qualifier",
        "TIER4ID":          "detail.**.interchange_control_header_NM1[entity_identifier_code='85'].billing_provider_id",
        "TIER4DESC":        "detail.**.interchange_control_header_NM1[entity_identifier_code='85'].billing_provider_name",
        "TIER4ADDRESS1":    "detail.**.interchange_control_header_N3[0].address_line_1",
        "TIER4ADDRESS2":    "detail.**.interchange_control_header_N3[0].address_line_2",
        "TIER4CITY":        "detail.**.interchange_control_header_N4[0].city",
        "TIER4STATE":       "detail.**.interchange_control_header_N4[0].state",
        "TIER4ZIP":         "detail.**.interchange_control_header_N4[0].zip_code",

        "STARTDATE":        "heading.**.interchange_control_header_BHT.bht04_04",
        "ENDDATE":          "detail.**.interchange_control_header_DTP[date_qualifier='008'].date_value",

        # =====================================================================
        # 2. PROVIDER DETAIL FIELDS (CamelCase KEYS) - for 837 compatibility
        # 837 JSON uses detail.submitter_NM1_loop.transaction_set_header_NM1
        # with entity 82 (rendering provider) or entity 85 (billing provider)
        # =====================================================================
        "ProviderID":                "detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_id ? detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_id : detail.**.transaction_set_header_NM1[entity_identifier_code='85'].billing_provider_id",
        "LastName":                  "detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_last_name ? detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_last_name : detail.**.transaction_set_header_NM1[entity_identifier_code='85'].billing_provider_name",
        "FirstName":                 "detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_first_name",
        "MiddleInitial":             "detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_middle_name",

        "TaxonomyCode1":             "(detail.**.transaction_set_header_PRV[prv01='PE'].prv03_03)[0]",
        "HpSpecialtyCode1":          "detail.unmapped",
        "ADVProviderSpecialtyCode1": "detail.unmapped",

        "TaxonomyCode2":             "(detail.**.transaction_set_header_PRV[prv01='PE'].prv03_03)[1]",
        "HpSpecialtyCode2":          "detail.unmapped",
        "ADVProviderSpecialtyCode2": "detail.unmapped",

        "TaxonomyCode3":             "(detail.**.transaction_set_header_PRV[prv01='PE'].prv03_03)[2]",
        "HpSpecialtyCode3":          "detail.unmapped",
        "ADVProviderSpecialtyCode3": "detail.unmapped",

        "TaxonomyCode4":             "(detail.**.transaction_set_header_PRV[prv01='PE'].prv03_03)[3]",
        "HpSpecialtyCode4":          "detail.unmapped",
        "ADVProviderSpecialtyCode4": "detail.unmapped",

        "TaxonomyCode5":             "(detail.**.transaction_set_header_PRV[prv01='PE'].prv03_03)[4]",
        "HpSpecialtyCode5":          "detail.unmapped",
        "ADVProviderSpecialtyCode5": "detail.unmapped",

        "NPI":                       "detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_id ? detail.**.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_id : detail.**.transaction_set_header_NM1[entity_identifier_code='85'].billing_provider_id",
        "PrescribePrivilege":        "detail.unmapped",
        "DEA":                       "**.transaction_set_header_REF.subluxation_documentation[0]",
        "PayorID":                   "(**.transaction_set_header_REF[reference_qualifier='PQ'].reference_value)[0]",
        "Contracted":                "detail.unmapped",
        "ProviderHAI":               "detail.unmapped",
        "HospitalID":                "detail.unmapped",
        "ExcludeFromProviderReporting": "detail.unmapped",

        "AltProvReporting1":         "(**.transaction_set_header_REF.blue_cross_provider_id)[0]",
        "AltProvReporting2":         "(**.transaction_set_header_REF.blue_shield_provider_id)[0]",
        "AltProvReporting3":         "detail.unmapped",
        "AltProvReporting4":         "detail.unmapped",
        "AltProvReporting5":         "detail.unmapped",
        "AltProvReporting6":         "detail.unmapped",
        "AltProvReporting7":         "detail.unmapped",
        "AltProvReporting8":         "detail.unmapped",
        "AltProvReporting9":         "detail.unmapped",
        "AltProvReporting10":        "detail.unmapped"
    }
}
