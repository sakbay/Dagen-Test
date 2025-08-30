# DBT Configuration Fix - Dagen-Test Project

## Issue Identified
The dbt job was failing with the following error:
```
Runtime Error
  at path ['name']: 'Dagen-Test' does not match '^[^\\d\\W]\\w*$'
```

## Root Cause
The project name `'Dagen-Test'` contained a hyphen character (`-`), which is not allowed in dbt project names. DBT requires project names to follow the regex pattern `'^[^\\d\\W]\\w*$'`, which means:
- Must start with a letter or underscore (not a digit or special character)
- Can only contain letters, numbers, and underscores
- Cannot contain hyphens, spaces, or other special characters

## Solution Applied
Updated the following files to use `dagen_test` instead of `Dagen-Test`:

### 1. `dbt_project.yml`
```yaml
# BEFORE
name: 'Dagen-Test'
profile: 'Dagen-Test'
models:
  Dagen-Test:

# AFTER  
name: 'dagen_test'
profile: 'dagen_test'
models:
  dagen_test:
```

### 2. `profiles.yml`
```yaml
# BEFORE
Dagen-Test:
  outputs:
    dev:
      # ... configuration

# AFTER
dagen_test:
  outputs:
    dev:
      # ... configuration
```

## Validation
- ✅ Configuration syntax now complies with dbt naming requirements
- ✅ All existing models and data remain intact in BigQuery
- ✅ Star schema tables are still accessible at `prd-dagen.payments_v1`
- ✅ Test queries execute successfully

## Next Steps
The dbt job should now run successfully. The project maintains all functionality while conforming to dbt's naming conventions.

## Files Modified
- `dbt_project.yml` - Updated project name and model configuration
- `profiles.yml` - Updated profile name to match project
- Backup files created: 
  - `dbt_project.yml.backup.1756577974`
  - `profiles.yml.backup.1756577995`

## Impact
- ✅ No impact on existing data or models
- ✅ No impact on BigQuery schema or tables
- ✅ No impact on business logic or transformations
- ✅ Only configuration naming convention compliance