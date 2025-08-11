{{
    config(
        labels={"tier": "silver"},
        materialized='incremental',
        unique_key='download_date',
        partition_by={
            "field": "download_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=['project_name'],
        description='Daily aggregated download metrics for PyMC packages with enrichment and business logic.',
    )
}}

WITH daily_downloads AS (
    SELECT
        DATE(timestamp) AS download_date,
        file.project AS project_name,
        file.version AS package_version,
        file.type AS file_type,
        details.installer.name AS installer_name,
        details.python AS python_version,
        details.system.name AS system_name,
        COUNT(*) AS total_downloads,
        COUNT(DISTINCT details.installer.name) AS unique_installers,
        COUNT(DISTINCT details.python) AS unique_python_versions,
        COUNT(DISTINCT details.system.name) AS unique_systems,
        COUNT(DISTINCT file.version) AS unique_package_versions
    FROM {{ ref('bronze_numfocus_projects') }}
    WHERE file.project = 'pymc3'
    AND DATE(timestamp) BETWEEN
        DATE_SUB(CURRENT_DATE, INTERVAL 366 DAY)
        AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    
    {% if is_incremental() %}
    AND DATE(timestamp) BETWEEN
        DATE_SUB(CURRENT_DATE, INTERVAL 2 DAY)
        AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
    {% endif %}
    
    GROUP BY
        DATE(timestamp),
        file.project,
        file.version,
        file.type,
        details.installer.name,
        details.python,
        details.system.name
),

enriched_daily_summary AS (
    SELECT
        download_date,
        project_name,
        SUM(total_downloads) AS daily_downloads,
        COUNT(DISTINCT package_version) AS versions_downloaded,
        COUNT(DISTINCT file_type) AS file_types_downloaded,
        COUNT(DISTINCT installer_name) AS unique_installers,
        COUNT(DISTINCT python_version) AS unique_python_versions,
        COUNT(DISTINCT system_name) AS unique_systems,
        AVG(SUM(total_downloads)) OVER (
            PARTITION BY project_name 
            ORDER BY download_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7day_avg_downloads,
        
        CURRENT_TIMESTAMP() AS last_updated
        
    FROM daily_downloads
    GROUP BY download_date, project_name
)

SELECT * FROM enriched_daily_summary
