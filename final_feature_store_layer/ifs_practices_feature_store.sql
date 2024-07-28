select

    /* Primary Key */
    , practices.practice_odu_id

    /* Foreign Keys */
    , 1 as all_practices_id

    /* Client Financial Quantiles */
    , practices_financials.quantile_10_hd_revenue_to_vso as quantile_10_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_25_hd_revenue_to_vso as quantile_25_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_50_hd_revenue_to_vso as quantile_50_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_75_hd_revenue_to_vso as quantile_75_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_90_hd_revenue_to_vso as quantile_90_transaction_hd_revenue_to_vso_all_clients_at_clinic

    , practices_financials.quantile_10_hd_revenue_to_practice as quantile_10_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_25_hd_revenue_to_practice as quantile_25_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_50_hd_revenue_to_practice as quantile_50_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_75_hd_revenue_to_practice as quantile_75_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_90_hd_revenue_to_practice as quantile_90_transaction_hd_revenue_to_practice_all_clients_at_clinic

    , practices_financials.quantile_10_inclinic_revenue_to_practice as quantile_10_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_25_inclinic_revenue_to_practice as quantile_25_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_50_inclinic_revenue_to_practice as quantile_50_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_75_inclinic_revenue_to_practice as quantile_75_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_90_inclinic_revenue_to_practice as quantile_90_transaction_inclinic_revenue_to_practice_all_clients_at_clinic


from {{ ref('ifs_practices') }} as practices
left join {{ ref('ifs_practices_financials') }} as practices_financials on practices.practice_odu_id = practices_financials.practice_odu_id
