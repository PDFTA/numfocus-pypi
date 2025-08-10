{{
    config(
        labels={"tier": "bronze"},
        materialized='incremental',
        cluster_by=['project_name'],
        description='Metadata about the NumFOCUS packages from the GBQ public dataset.',)
}}
SELECT 
  name AS project_name
  , version
  , author_email
  , maintainer_email
  , filename
  , upload_time
FROM {{ source("pypi", "distribution_metadata") }}
WHERE name = "numpy"