{{
    config(
        unique_key = 'client_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}

select

    1 as finance_id

    /* HD Revenue to VSO */
    , approx_percentile(clients.total_hd_revenue_to_vso,0.1) as client_quantile_10_total_hd_revenue_to_vso
    , approx_percentile(clients.total_hd_revenue_to_vso,0.25) as client_quantile_25_total_hd_revenue_to_vso
    , approx_percentile(clients.total_hd_revenue_to_vso,0.5) as client_quantile_50_total_hd_revenue_to_vso
    , approx_percentile(clients.total_hd_revenue_to_vso,0.75) as client_quantile_75_total_hd_revenue_to_vso
    , approx_percentile(clients.total_hd_revenue_to_vso,0.9) as client_quantile_90_total_hd_revenue_to_vso

    /* HD Revenue to Practice */
    , approx_percentile(clients.total_hd_revenue_to_practice,0.1) as client_quantile_10_total_hd_revenue_to_practice
    , approx_percentile(clients.total_hd_revenue_to_practice,0.25) as client_quantile_25_total_hd_revenue_to_practice
    , approx_percentile(clients.total_hd_revenue_to_practice,0.5) as client_quantile_50_total_hd_revenue_to_practice
    , approx_percentile(clients.total_hd_revenue_to_practice,0.75) as client_quantile_75_total_hd_revenue_to_practice
    , approx_percentile(clients.total_hd_revenue_to_practice,0.9) as client_quantile_90_total_hd_revenue_to_practice

    /* HD In-Clinic Revenue */
    , approx_percentile(clients.total_inclinic_revenue_to_practice,0.1) as client_quantile_10_total_inclinic_revenue_to_practice
    , approx_percentile(clients.total_inclinic_revenue_to_practice,0.25) as client_quantile_25_total_inclinic_revenue_to_practice
    , approx_percentile(clients.total_inclinic_revenue_to_practice,0.5) as client_quantile_50_total_inclinic_revenue_to_practice
    , approx_percentile(clients.total_inclinic_revenue_to_practice,0.75) as client_quantile_75_total_inclinic_revenue_to_practice
    , approx_percentile(clients.total_inclinic_revenue_to_practice,0.9) as client_quantile_90_total_inclinic_revenue_to_practice

from {{ ref('ifs_clients_financials') }} as clients
