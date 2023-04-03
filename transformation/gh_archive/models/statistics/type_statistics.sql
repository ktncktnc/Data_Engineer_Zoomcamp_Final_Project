{{
    config(materialized='incremental')
}}

select
    CAST(DATE(created_at) AS DATE) as `report_date`,
    `type` as event_type,
    count(*) as total
from {{ ref("rawlog_staging") }}
group by `report_date`, `type`
