
{{
    config(
        unique_key = 'transaction_odu_id'
        , cluster_by = 'server_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}

select
    transactions_inclinic.*
from {{ ref('transactions_inclinic') }} as transactions_inclinic
inner join {{ ref('ifs_practices') }} as complete_integrated_practices on transactions_inclinic.practice_odu_id = complete_integrated_practices.practice_odu_id
left join {{ ref('dates') }} as dates on transactions_inclinic.reporting_date = dates.date
where dates.is_last_24_complete_months
