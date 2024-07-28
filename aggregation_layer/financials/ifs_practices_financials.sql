{{
    config(
        unique_key = 'practice_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}


with online_financials as (
    select

        transactions.practice_odu_id

        /* Total Transactions */
        , count(distinct transactions.transaction_odu_id) as total_hd_transactions
        , count(distinct case when dates.is_last_12_complete_months then transactions.transaction_odu_id end) as total_hd_transactions_last_12_months
        , count(distinct case when dates.is_last_24_complete_months then transactions.transaction_odu_id end) as total_hd_transactions_last_24_months

        /* HD Revenue to VSO */
        , sum(revenue_to_vso) as total_hd_revenue_to_vso
        , approx_percentile(revenue_to_vso,0.1) as quantile_10_hd_revenue_to_vso
        , approx_percentile(revenue_to_vso,0.25) as quantile_25_hd_revenue_to_vso
        , approx_percentile(revenue_to_vso,0.5) as quantile_50_hd_revenue_to_vso
        , approx_percentile(revenue_to_vso,0.75) as quantile_75_hd_revenue_to_vso
        , approx_percentile(revenue_to_vso,0.9) as quantile_90_hd_revenue_to_vso
        , sum(case when dates.is_last_12_complete_months then revenue_to_vso end) as total_hd_revenue_to_vso_last_12_months
        , sum(case when dates.is_last_24_complete_months then revenue_to_vso end) as total_hd_revenue_to_vso_last_24_months

        /* HD Revenue to Practice */
        , sum(revenue_to_practice) as total_hd_revenue_to_practice
        , approx_percentile(revenue_to_practice,0.1) as quantile_10_hd_revenue_to_practice
        , approx_percentile(revenue_to_practice,0.25) as quantile_25_hd_revenue_to_practice
        , approx_percentile(revenue_to_practice,0.5) as quantile_50_hd_revenue_to_practice
        , approx_percentile(revenue_to_practice,0.75) as quantile_75_hd_revenue_to_practice
        , approx_percentile(revenue_to_practice,0.9) as quantile_90_hd_revenue_to_practice
        , sum(case when dates.is_last_12_complete_months then revenue_to_practice end) as total_hd_revenue_to_practice_last_12_months
        , sum(case when dates.is_last_24_complete_months then revenue_to_practice end) as total_hd_revenue_to_practice_last_24_months

    from {{ ref('ifs_transactions_online_v2') }} as transactions
    left join {{ ref('client_online_details') }} as client_online_details on transactions.visitor_vso_id = client_online_details.visitor_vso_id
    left join {{ ref('ifs_clients_autoship') }} as autoships on transactions.client_odu_id = autoships.client_odu_id
    left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
    where is_hd
        and is_vso_debit
    group by all
)

, inclinic_financials as (
    select

        transactions.practice_odu_id

        /* Total Transactions */
        , count(distinct transactions.transaction_odu_id) as total_inclinic_transactions
        , count(distinct case when dates.is_last_12_complete_months then transactions.transaction_odu_id end) as total_inclinic_transactions_last_12_months
        , count(distinct case when dates.is_last_24_complete_months then transactions.transaction_odu_id end) as total_inclinic_transactions_last_24_months

        /* Revenue to Practice */
        , sum(transactions.reporting_amount) as total_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.1) as quantile_10_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.25) as quantile_25_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.5) as quantile_50_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.75) as quantile_75_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.9) as quantile_90_reporting_amount
        , sum(case when dates.is_last_12_complete_months then transactions.reporting_amount end) as total_reporting_amount_last_12_months
        , sum(case when dates.is_last_24_complete_months then transactions.reporting_amount end) as total_reporting_amount_last_24_months

    from {{ ref('ifs_transactions_inclinic') }} as transactions
    left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
    left join {{ ref('ifs_transactions') }} as transactions_wellness on transactions.transaction_odu_id = transactions_wellness.transaction_odu_id
    group by 1
)

select
    coalesce(online_financials.practice_odu_id, inclinic_financials.practice_odu_id) as practice_odu_id
    , online_financials.total_hd_transactions
    , online_financials.total_hd_transactions_last_12_months
    , online_financials.total_hd_transactions_last_24_months
    , online_financials.total_hd_revenue_to_vso
    , online_financials.quantile_10_hd_revenue_to_vso
    , online_financials.quantile_25_hd_revenue_to_vso
    , online_financials.quantile_50_hd_revenue_to_vso
    , online_financials.quantile_75_hd_revenue_to_vso
    , online_financials.quantile_90_hd_revenue_to_vso
    , online_financials.total_hd_revenue_to_vso_last_12_months
    , online_financials.total_hd_revenue_to_vso_last_24_months
    , online_financials.total_hd_revenue_to_practice
    , online_financials.quantile_10_hd_revenue_to_practice
    , online_financials.quantile_25_hd_revenue_to_practice
    , online_financials.quantile_50_hd_revenue_to_practice
    , online_financials.quantile_75_hd_revenue_to_practice
    , online_financials.quantile_90_hd_revenue_to_practice
    , online_financials.total_hd_revenue_to_practice_last_12_months
    , online_financials.total_hd_revenue_to_practice_last_24_months
    , inclinic_financials.total_inclinic_transactions
    , inclinic_financials.total_inclinic_transactions_last_12_months
    , inclinic_financials.total_inclinic_transactions_last_24_months
    , inclinic_financials.total_reporting_amount as total_inclinic_revenue_to_practice
    , inclinic_financials.quantile_10_reporting_amount as quantile_10_inclinic_revenue_to_practice
    , inclinic_financials.quantile_25_reporting_amount as quantile_25_inclinic_revenue_to_practice
    , inclinic_financials.quantile_50_reporting_amount as quantile_50_inclinic_revenue_to_practice
    , inclinic_financials.quantile_75_reporting_amount as quantile_75_inclinic_revenue_to_practice
    , inclinic_financials.quantile_90_reporting_amount as quantile_90_inclinic_revenue_to_practice
    , inclinic_financials.total_reporting_amount_last_12_months as total_inclinic_revenue_to_practice_last_12_months
    , inclinic_financials.total_reporting_amount_last_24_months as total_inclinic_revenue_to_practice_last_24_months
from online_financials
full outer join inclinic_financials on online_financials.practice_odu_id = inclinic_financials.practice_odu_id
