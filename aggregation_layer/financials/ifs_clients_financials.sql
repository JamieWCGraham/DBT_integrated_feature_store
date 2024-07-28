{{
    config(
        unique_key = 'client_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}


with online_financials as (
    select

        transactions.client_odu_id
        , transactions.practice_odu_id

        /* Total Transactions */
        , count(distinct transactions.transaction_odu_id) as total_hd_transactions
        , count(distinct case when dates.is_last_12_complete_months then transactions.transaction_odu_id end) as total_hd_transactions_last_12_months
        , count(distinct case when dates.is_last_24_complete_months then transactions.transaction_odu_id end) as total_hd_transactions_last_24_months

        /* Recency and Frequency */
        , datediff(day,max(reporting_date),current_date) as days_since_last_hd_transaction
        , datediff(day,min(reporting_date),max(reporting_date)) as hd_lifespan_in_days

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

        /* Autoships */
        , client_online_details.is_autoship_customer
        , sum(case when is_autoship then revenue_to_vso end) as total_autoship_revenue
        , sum(case when is_autoship and dates.is_last_12_complete_months then revenue_to_vso end) as total_autoship_revenue_last_12_months
        , sum(case when is_autoship and dates.is_last_24_complete_months then revenue_to_vso end) as total_autoship_revenue_last_24_months
        , autoships.is_autoship_active

    from {{ ref('ifs_transactions_online_v2') }} as transactions
    inner join {{ ref('ifs_clients') }} as clients on transactions.client_odu_id = clients.client_odu_id and transactions.practice_odu_id = clients.practice_odu_id
    left join {{ ref('client_online_details') }} as client_online_details on transactions.visitor_vso_id = client_online_details.visitor_vso_id
    left join {{ ref('ifs_clients_autoship') }} as autoships on transactions.client_odu_id = autoships.client_odu_id
    left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
    where is_hd
        and is_vso_debit
    group by all
)

, inclinic_financials as (
    select

        transactions.client_odu_id
        , transactions.practice_odu_id

        /* Total Transactions */
        , count(distinct transactions.transaction_odu_id) as total_inclinic_transactions
        , count(distinct case when dates.is_last_12_complete_months then transactions.transaction_odu_id end) as total_inclinic_transactions_last_12_months
        , count(distinct case when dates.is_last_24_complete_months then transactions.transaction_odu_id end) as total_inclinic_transactions_last_24_months

        /* Recency and Frequency */
        , datediff(day,max(transactions.reporting_date),current_date) as days_since_last_inclinic_transaction
        , datediff(day,min(transactions.reporting_date),max(transactions.reporting_date)) as inclinic_lifespan_in_days

        /* Revenue to Practice */
        , sum(transactions.reporting_amount) as total_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.1) as quantile_10_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.25) as quantile_25_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.5) as quantile_50_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.75) as quantile_75_reporting_amount
        , approx_percentile(transactions.reporting_amount,0.9) as quantile_90_reporting_amount
        , sum(case when dates.is_last_12_complete_months then transactions.reporting_amount end) as total_reporting_amount_last_12_months
        , sum(case when dates.is_last_24_complete_months then transactions.reporting_amount end) as total_reporting_amount_last_24_months

        /* Other */
        , count(distinct case when transactions.top_revenue_category_name = 'Drug and Medications' then transactions.transaction_odu_id end ) as total_drug_and_medications_transactions
        , total_drug_and_medications_transactions > 0 as has_drug_and_medications
        , sum(case when transactions.top_revenue_category_name = 'Drug and Medications' then transactions.reporting_amount end ) as total_drug_and_medications_revenue
        , mode(transactions.top_revenue_category_name) as top_inclinic_revenue_category_name


    from {{ ref('ifs_transactions_inclinic') }} as transactions
    inner join {{ ref('ifs_clients') }} as clients on transactions.client_odu_id = clients.client_odu_id and transactions.practice_odu_id = clients.practice_odu_id
    left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
    left join {{ ref('ifs_transactions') }} as transactions_wellness on transactions.transaction_odu_id = transactions_wellness.transaction_odu_id
    group by all
)

select
    coalesce(online_financials.client_odu_id, inclinic_financials.client_odu_id) as client_odu_id
    , coalesce(online_financials.practice_odu_id, inclinic_financials.practice_odu_id) as practice_odu_id
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
    , online_financials.days_since_last_hd_transaction
    , online_financials.hd_lifespan_in_days
    , online_financials.total_autoship_revenue
    , online_financials.total_autoship_revenue_last_12_months
    , online_financials.total_autoship_revenue_last_24_months
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
    , inclinic_financials.days_since_last_inclinic_transaction
    , inclinic_financials.inclinic_lifespan_in_days
    , inclinic_financials.has_drug_and_medications
    , inclinic_financials.total_drug_and_medications_revenue
    , inclinic_financials.total_drug_and_medications_transactions
    , inclinic_financials.top_inclinic_revenue_category_name
from online_financials
full outer join inclinic_financials on online_financials.client_odu_id = inclinic_financials.client_odu_id
