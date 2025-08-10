{{
    config(
        labels={"tier": "bronze"},
        materialized='incremental',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day",
        },
        description='NumFOCUS downloads data from the GBQ public dataset.',
    )
}}
SELECT
    *,
FROM {{ source("pypi", "file_downloads") }}
WHERE file_downloads.file.project IN ('pymc3')
AND timestamp BETWEEN
        TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL + 366 DAY))
         AND TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL +1 DAY))

{% if is_incremental() %}

AND timestamp BETWEEN
        TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL + 2 DAY))
         AND TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL +1 DAY))

{% endif %}
