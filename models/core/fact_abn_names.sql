{{
    config(
        materialized='table'
    )
}}

select 
main_data.bn_name,
main_data.bn_status,
main_data.bn_reg_dt,
extract(year from main_data.bn_reg_dt) as RegisteredYear,
main_data.abn,
additional.state,
additional.entitytype,
entity.description
from {{ ref('stg_abn_names') }} as main_data
left join {{ ref('stg_additional_info') }} as additional
on main_data.abn=additional.abn
left join {{ ref('dim_entities') }} as entity
on entity.Code=additional.entitytype
where extract(year from main_data.bn_reg_dt) between 2014 and 2023
and additional.state is not null
and additional.entitytype is not null