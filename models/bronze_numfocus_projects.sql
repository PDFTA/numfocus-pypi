{{
    config(
        labels={"tier": "bronze"},
        description='NumFOCUS downloads data from the GBQ public dataset.',
    )
}}
WITH numfocus_data AS (
    SELECT
        *,
    FROM {{ source("pypi", "file_downloads") }}
    WHERE TIMESTAMP_TRUNC(timestamp, DAY) = TIMESTAMP("2025-04-16")
        AND file_downloads.file.project IN ('pymc3')

)

SELECT *
FROM numfocus_data
LIMIT 100