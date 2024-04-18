{{
    config(
        materialized='view'
    )
}}

select
    bn_name,
    bn_status,
    bn_reg_dt,
    bn_cancel_dt,
    bn_renew_dt,
    cast(bn_abn as string) as abn

from {{ source('staging', 'abn_names') }}
where bn_abn is not null

