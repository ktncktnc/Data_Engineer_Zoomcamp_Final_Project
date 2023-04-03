{{
    config(materialized='view')
}}

select
    *
from
    {{ source('gh_archive', 'raw_log')}}