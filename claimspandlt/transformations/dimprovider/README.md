# DimProvider DLT Pipeline

## Overview

This DLT (Delta Live Tables) pipeline processes provider dimension data from EDI files through Bronze and Silver layers following the Medallion architecture.

## Architecture

```
EDI Source Files (837, 274)
    ↓
edi_parsed_mapped (upstream)
    ↓
┌─────────────────────────────────────────────┐
│           BRONZE LAYER                      │
│  bronze_provider.py                         │
├─────────────────────────────────────────────┤
│  1. bronze_provider_processed               │
│     - 837 claims with provider info         │
│     - Schema validation                     │
│                                             │
│  2. provider_consolidated                   │
│     - Consolidation mapping                 │
│     - Standardized fields                   │
│                                             │
│  3. bronze_provider_hierarchy_processed     │
│     - 274 provider hierarchy                │
│     - Schema validation                     │
│                                             │
│  4. provider_hierarchy_consolidated         │
│     - Hierarchy consolidation               │
│     - Multi-tier relationships              │
└─────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────┐
│           SILVER LAYER                      │
│  silver_provider.sql                        │
│  silver_provider_hierarchy.sql              │
├─────────────────────────────────────────────┤
│  1. silver_provider_person_bridge           │
│     - Unique provider identifiers           │
│     - Provider linkages (NPI, ID, PayerID)  │
│                                             │
│  2. silver_provider                         │
│     - Enriched provider dimension           │
│     - Taxonomy & specialty codes            │
│     - Reference data lookups                │
│                                             │
│  3. silver_provider_hierarchy               │
│     - Multi-tier location hierarchy         │
│     - Organizational relationships          │
└─────────────────────────────────────────────┘
```

## Files

### Bronze Layer
* **bronze_provider.py**: Main bronze processing logic
  * Reads from `edi_parsed_mapped` streaming table
  * Filters for layout IDs: 837 (provider), 274 (hierarchy)
  * Applies schema validation
  * Executes consolidation transformations

### Silver Layer
* **silver_provider.sql**: Provider enrichment
  * Creates provider person bridge for unique identifiers
  * Enriches with taxonomy codes and specialty lookups
  * Joins with reference tables:
    * `ref_careprecise_taxonomy` - Taxonomy to specialty mapping
    * `ref_credentialing` - Credentialing information

* **silver_provider_hierarchy.sql**: Hierarchy processing
  * Processes multi-tier organizational structure
  * Handles location, group, and tier relationships
  * Maintains address and contact information

## Schema Locations

The pipeline references schemas from the original ClaimsProcessing directory:

```
/ClaimsProcessing/dimProvider/Bronze/Schema/
  ├── provider_schema.json
  ├── provider_hierarchy_schema.json
  └── Consolidation/
      ├── ConsolidationProvider.json
      ├── ConsolidationProviderHierarchy.json
      └── DataModels/
          ├── providerdatamodel.json
          └── providerhierarchydatamodel.json
```

## Prerequisites

### Upstream Dependencies
1. **edi_parsed_mapped** table must exist (from edi_ingestion.py)
2. Schema files must be present in ClaimsProcessing folder
3. Shared processing libraries:
   * `src.shared.filestoprocess.FilesToProcess`
   * `src.utils.filehandling.FileHandler`
   * `src.shared.consolidation.ConsolidationProcessor`

### Reference Tables (Silver Layer)
* `claimspan.silver.ref_careprecise_taxonomy`
* `claimspan.silver.ref_credentialing`

## Key Datasets

### Bronze Layer Tables

| Table | Type | Description |
|-------|------|-------------|
| bronze_provider_processed | Streaming | 837 provider records with schema validation |
| provider_consolidated | Streaming | Consolidated provider data |
| bronze_provider_hierarchy_processed | Streaming | 274 hierarchy records |
| provider_hierarchy_consolidated | Streaming | Consolidated hierarchy data |

### Silver Layer Tables

| Table | Type | Description |
|-------|------|-------------|
| silver_provider_person_bridge | Streaming | Provider identifier linkage |
| silver_provider | Streaming | Enriched provider dimension |
| silver_provider_hierarchy | Streaming | Multi-tier hierarchy |

## Field Mappings

### Provider Fields
* **Identifiers**: ProviderID, NPI, PayerID, DEA, HAI
* **Name**: LastName, FirstName, MiddleInitial
* **Specialty Codes** (5 sets):
  * TaxonomyCode1-5
  * HpSpecialtyCode1-5
  * ADVProviderSpecialtyCode1-5
* **Extensions**: PrescribePrivilege, Contracted, ExcludeFromReporting
* **Alternative IDs**: AltProvReporting1-10

### Hierarchy Fields
* **Provider**: ProviderID, ProviderLastName, ProviderNPI
* **Location (Tier 1)**: LocationID, Address, City, State, Zip, TIN
* **Tier 2-4**: ID, Description, Address components
* **Dates**: StartDate, EndDate

## Usage

### Adding to Pipeline

1. Include the files in your DLT pipeline configuration:
```python
libraries = [
    {"file": {"path": "/Repos/.../claimspandlt/revenuegap/dimprovider/bronze_provider.py"}},
    {"file": {"path": "/Repos/.../claimspandlt/revenuegap/dimprovider/silver_provider.sql"}},
    {"file": {"path": "/Repos/.../claimspandlt/revenuegap/dimprovider/silver_provider_hierarchy.sql"}}
]
```

2. Ensure CLAIMSPAN_REPO_ROOT environment variable is set:
```python
configuration = {
    "CLAIMSPAN_REPO_ROOT": "/Workspace/Repos/logi@openhealthagents.org/claimspan"
}
```

3. Set target catalog and schema:
```python
target = "claimspan.silver"
```

### Running the Pipeline

The pipeline processes data incrementally:
* Bronze layer reads from streaming EDI source
* Silver layer enriches with reference data
* All transformations maintain streaming semantics

## Data Quality

* Schema validation at bronze layer
* Reference data lookups for taxonomy codes
* Provider identifier deduplication
* Multi-tier hierarchy validation

## Troubleshooting

### Common Issues

1. **Schema not found**
   * Verify ClaimsProcessing folder structure
   * Check CLAIMSPAN_REPO_ROOT environment variable

2. **Reference tables missing**
   * Ensure `ref_careprecise_taxonomy` table exists
   * Ensure `ref_credentialing` table exists

3. **Upstream dependency**
   * Confirm `edi_parsed_mapped` table is available
   * Check if EDI ingestion pipeline is running

## Notes

* Layout ID 837: Professional claims with provider information
* Layout ID 274: Provider hierarchy/affiliation files
* All tables use streaming semantics for incremental processing
* Bronze layer performs consolidation before silver enrichment
