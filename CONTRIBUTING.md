# Contributing to AlphaesAI Claims Processing

Thank you for your interest in contributing to the AlphaesAI Claims Processing project! This guide will help you set up your development environment and understand our workflow.

## Prerequisites

* A Databricks account (Free Edition or higher)
* Git account (GitHub)
* Basic knowledge of Python, SQL, and EDI file formats

## Setup Instructions

### 1. Create a Databricks Account

1. Go to [databricks.com](https://www.databricks.com/) and sign up for a free account
2. **Important for Free Edition users**: During setup, select the **us-east-2** region (Ohio)
   * Free Edition has region restrictions; us-east-2 provides the best compatibility
3. Complete the account creation process

### 2. Clone the Repository

1. In your Databricks workspace, navigate to **Repos** in the left sidebar
2. Click **Create Repo** (or **Add Repo**)
3. Select **Clone from Git**
4. Enter the repository URL:
   ```
   https://github.com/alphaesAI/alphaesai.git
   ```
5. Click **Create Repo**

The repository will be cloned to: `/Repos/<your-email@domain.com>/alphaesai/`

### 3. Install Dependencies

Since Databricks serverless compute resets between sessions, you'll need to install dependencies each time you start working:

1. Open a **Databricks notebook** or use the **Terminal** (if available in your edition)
2. Run the following command:
   ```bash
   pip install pyedi
   ```

**Note**: You must reinstall this package every time your compute environment restarts or when you begin a new session.

### 4. Update Workspace Paths

Before running any pipeline, you **must** update the `ROOT_DIR` variable to match your workspace path:

1. Open any orchestrator notebook (e.g., `Member_Pipeline`, `qualitymeasure_pipeline`, `dimgapsincare_pipeline`)
2. Find the cell that defines `ROOT_DIR` (usually Cell 2)
3. Update the path to match your workspace:
   ```python
   # Original (example):
   ROOT_DIR = Path("/Workspace/Repos/logi@openhealthagents.org/alphaesai/ClaimsProcessing")
   
   # Update to your path:
   ROOT_DIR = Path("/Workspace/Repos/<your-email@domain.com>/alphaesai/ClaimsProcessing")
   ```
4. Save the notebook

**Important**: Update `ROOT_DIR` in **every orchestrator notebook** under each `Dim*` folder:
* `DimMember/Member_Pipeline`
* `DimQualityMeasure/qualitymeasure_pipeline`
* `DimGapsInCare/dimgapsincare_pipeline`
* Any other pipeline notebooks you plan to run

### 5. Set Up Unity Catalog Volumes

Create the required Unity Catalog volumes for Bronze layer processing:

1. Create the following volumes:
   * `/Volumes/claimsprocessing/bronze/member`
   * `/Volumes/claimsprocessing/bronze/member_consolidated`

**Note**: Other tables and volumes will be created automatically at runtime by the pipelines.

### 6. Create Source Directory Structure

The pipelines require specific folders for EDI file processing. Ensure these directories exist:

```
ClaimsProcessing/source/834/
├── pending/       # Place EDI files here for processing
├── inprogress/    # Files currently being processed
├── failed/        # Files that failed processing
└── processed/     # Successfully processed files
```

To create these directories, run the following in a notebook cell:
```python
from pathlib import Path

ROOT_DIR = Path("/Workspace/Repos/<your-email@domain.com>/alphaesai/ClaimsProcessing")
source_dirs = ["pending", "inprogress", "failed", "processed"]

for dir_name in source_dirs:
    (ROOT_DIR / "source/834" / dir_name).mkdir(parents=True, exist_ok=True)
    print(f"✓ Created: source/834/{dir_name}")
```

## Project Structure

```
alphaesai/
├── ClaimsProcessing/
│   ├── DDL/                        # Database schema definitions
│   ├── Shared/
│   │   ├── EDIProcessing/          # EDI parsing and CSV conversion
│   │   ├── Notebooks/              # Shared orchestration notebooks
│   │   └── CommonMethods/          # Helper utilities
│   ├── DimMember/                  # Member dimension processing
│   │   ├── Bronze/                 # Raw data processing
│   │   │   └── Schema/             # Bronze schema definitions
│   │   ├── Silver/                 # Cleaned and validated data
│   │   │   └── Notebooks/
│   │   ├── Gold/                   # Business-ready dimensions
│   │   │   ├── Config/
│   │   │   ├── DataUpdate/
│   │   │   ├── DataProcessing/
│   │   │   └── Notebooks/
│   │   ├── EDIProcessing/          # Member-specific EDI mapping
│   │   └── Member_Pipeline.ipynb   # Full orchestrator
│   ├── DimQualityMeasure/          # Quality measure dimension (Gold only)
│   │   └── Gold/
│   │       ├── Config/
│   │       ├── DataUpdate/
│   │       ├── DataProcessing/
│   │       └── Notebooks/
│   ├── DimGapsInCare/              # Gaps in care dimension (Bronze + Silver)
│   │   ├── Bronze/
│   │   └── Silver/
│   │       ├── Config/
│   │       ├── DataUpdate/
│   │       ├── DataProcessing/
│   │       └── Notebooks/
│   └── DimQualityEvent/            # Quality event dimension
└── source/                         # EDI file drop location
    └── 834/
        ├── pending/                # Place EDI files here for processing
        ├── inprogress/             # Files being processed
        ├── failed/                 # Failed files
        └── processed/              # Completed files
```

## Development Workflow

### Processing EDI Files

1. Place EDI 834 files in: `/Workspace/Repos/<your-email>/alphaesai/ClaimsProcessing/source/834/pending/`
2. Run the appropriate pipeline orchestrator:
   * **Member data**: `DimMember/Member_Pipeline`
   * **Quality measures**: `DimQualityMeasure/qualitymeasure_pipeline`
   * **Gaps in care**: `DimGapsInCare/dimgapsincare_pipeline`
3. Monitor the console output for processing status
4. Check the appropriate Unity Catalog tables for results

### Making Changes

1. Create a new branch for your feature/fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes in the Databricks notebooks or files
3. Test your changes thoroughly:
   * Run the full pipeline end-to-end
   * Verify data quality in Bronze, Silver, and Gold layers
   * Check for any errors or warnings
4. Commit your changes:
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```
5. Push to your fork and submit a Pull Request

### Testing

Before submitting a PR:

1. **Unit tests**: Ensure your code passes existing tests
2. **Integration tests**: Run the full pipeline with sample data
3. **Data validation**: Verify output data quality and schema
4. **Performance**: Check that processing times are reasonable

## Common Issues and Solutions

### Issue: `pyedi` module not found
**Solution**: Reinstall the dependency:
```bash
pip install pyedi
```

### Issue: File path errors
**Solution**: Verify your `ROOT_DIR` is set correctly in all pipeline notebooks

### Issue: Permission denied on Unity Catalog
**Solution**: Check that you have the necessary permissions on catalogs and schemas. For Free Edition, ensure you're using the default metastore.

### Issue: Pipeline fails at Bronze/Silver/Gold layer
**Solution**: 
1. Check the error message in the failed notebook
2. Verify all required config files exist (`.json`, `.sql`)
3. Ensure upstream data is available

### Issue: Compute unavailable
**Solution**: Free Edition has usage limits. Wait for the limit to reset (daily) or upgrade to a paid plan.

### Issue: Source directories not found
**Solution**: Ensure you've created the required source folder structure (pending, inprogress, failed, processed) as described in the setup instructions.

## Code Style Guidelines

* **Python**: Follow PEP 8 style guidelines
* **SQL**: Use lowercase for keywords, snake_case for table/column names
* **Notebooks**: Keep cells focused on single tasks; use descriptive cell titles
* **Comments**: Document complex logic and business rules
* **Error handling**: Always wrap risky operations in try/except blocks

## Contributing Guidelines

1. **Branching**: Create feature branches from `main`
2. **Commits**: Write clear, descriptive commit messages
3. **Pull Requests**: Include a description of what changed and why
4. **Code Review**: Be responsive to feedback and questions
5. **Documentation**: Update README.md and CONTRIBUTING.md as needed

## Getting Help

* **Issues**: Open a GitHub issue for bugs or feature requests
* **Questions**: Use GitHub Discussions for general questions
* **Email**: Contact the maintainers at support@alphaesai.com (if applicable)

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to AlphaesAI Claims Processing! 🚀