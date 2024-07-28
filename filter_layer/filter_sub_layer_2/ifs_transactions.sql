{{
    config(
        unique_key = 'transaction_odu_id'
        , cluster_by = 'server_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}

select
    transactions.*
from {{ ref('transactions') }} as transactions
inner join {{ ref('ifs_practices') }} as complete_integrated_practices on transactions.practice_odu_id = complete_integrated_practices.practice_odu_id
left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
where dates.is_last_24_complete_months
