

{{
    config(
        unique_key = 'transaction_odu_id'
        , cluster_by = 'server_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}

select
    transactions_online.*
from {{ ref('transactions_online_v2') }} as transactions_online
inner join {{ ref('ifs_practices') }} as complete_integrated_practices on transactions_online.practice_odu_id = complete_integrated_practices.practice_odu_id
left join {{ ref('dates') }} as dates on transactions_online.reporting_date = dates.date
where (transactions_online.is_econnect or transactions_online.is_hd)
and dates.is_last_24_complete_months
