{{
    config(
        materialized='view'
    )
}}


select
    abn,
    entitytype,
    state

from {{ source('staging', 'additional_info') }}

