select

    /* Primary Key */
    clients.client_odu_id

    /* Foreign Keys */
    , clients.practice_odu_id
    , 1 as all_practices_id

    /* Channel Type */
    , clients.is_hd_client
    , clients.is_econnect_client
    , clients.is_inclinic_client

    /* Patient Metrics */
    , patients.total_pets
    , patients.minimum_pet_age
    , patients.maximum_pet_age
    , patients.average_pet_age
    , patients.has_dog
    , patients.has_cat
    , patients.species

    /* Demographics */
    , demographics.zipcode_household_average_income
    , demographics.zip

    /* Contact */
    , clients.is_safe_to_contact
    , demographics.is_email_available
    , demographics.is_phone_available

    /* EComm Website */
    , ecomm_website.last_signed_in_ecomm_website
    , ecomm_website.total_logins_last_12_months_ecomm_website

    /* Petmail */
    , petmail.has_petmail_generated_revenue
    , petmail.total_revenue_through_petmail
    , petmail.total_revenue_through_petmail_last_12_months
    , petmail.total_transactions_through_petmail
    , petmail.total_transactions_through_petmail_last_12_months

    /*** Financial Metrics ***/

    /* Purchase Types */
    , financials.top_inclinic_revenue_category_name
    , financials.has_drug_and_medications
    , financials.total_drug_and_medications_revenue
    , financials.total_drug_and_medications_transactions
    , spay_and_neuter.clinic_average_spay_and_neuter_cost
    , promotions.has_promo_usage
    , autoship.is_autoship_active
    , financials.total_autoship_revenue
    , financials.total_autoship_revenue_last_12_months
    , financials.total_autoship_revenue_last_24_months

    /* Revenue to VSO HD */
    , financials.total_hd_revenue_to_vso

    , financials.quantile_10_hd_revenue_to_vso as quantile_10_transaction_hd_revenue_to_vso
    , financials.quantile_25_hd_revenue_to_vso as quantile_25_transaction_hd_revenue_to_vso
    , financials.quantile_50_hd_revenue_to_vso as quantile_50_transaction_hd_revenue_to_vso
    , financials.quantile_75_hd_revenue_to_vso as quantile_75_transaction_hd_revenue_to_vso
    , financials.quantile_90_hd_revenue_to_vso as quantile_90_transaction_hd_revenue_to_vso

    , client_quantiles_total_revenue_per_practice.client_quantile_10_total_hd_revenue_to_vso as quantile_10_total_client_hd_revenue_to_vso_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_25_total_hd_revenue_to_vso as quantile_25_total_client_hd_revenue_to_vso_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_50_total_hd_revenue_to_vso as quantile_50_total_client_hd_revenue_to_vso_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_75_total_hd_revenue_to_vso as quantile_75_total_client_hd_revenue_to_vso_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_90_total_hd_revenue_to_vso as quantile_90_total_client_hd_revenue_to_vso_all_clients_at_clinic

    , client_quantiles_total_revenue_all_practices.client_quantile_10_total_hd_revenue_to_vso as quantile_10_total_client_hd_revenue_to_vso_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_25_total_hd_revenue_to_vso as quantile_25_total_client_hd_revenue_to_vso_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_50_total_hd_revenue_to_vso as quantile_50_total_client_hd_revenue_to_vso_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_75_total_hd_revenue_to_vso as quantile_75_total_client_hd_revenue_to_vso_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_90_total_hd_revenue_to_vso as quantile_90_total_client_hd_revenue_to_vso_all_clients_all_clinics

    , practices_financials.quantile_10_hd_revenue_to_vso as quantile_10_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_25_hd_revenue_to_vso as quantile_25_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_50_hd_revenue_to_vso as quantile_50_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_75_hd_revenue_to_vso as quantile_75_transaction_hd_revenue_to_vso_all_clients_at_clinic
    , practices_financials.quantile_90_hd_revenue_to_vso as quantile_90_transaction_hd_revenue_to_vso_all_clients_at_clinic

    , aggregate_financials_all_clients_all_practices.quantile_10_hd_revenue_to_vso as quantile_10_transaction_hd_revenue_to_vso_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_25_hd_revenue_to_vso as quantile_25_transaction_hd_revenue_to_vso_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_50_hd_revenue_to_vso as quantile_50_transaction_hd_revenue_to_vso_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_75_hd_revenue_to_vso as quantile_75_transaction_hd_revenue_to_vso_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_90_hd_revenue_to_vso as quantile_90_transaction_hd_revenue_to_vso_all_clients_all_clinics

    /* Revenue to Practice HD */
    , financials.total_hd_revenue_to_practice

    , financials.quantile_10_hd_revenue_to_practice as quantile_10_transaction_hd_revenue_to_practice
    , financials.quantile_25_hd_revenue_to_practice as quantile_25_transaction_hd_revenue_to_practice
    , financials.quantile_50_hd_revenue_to_practice as quantile_50_transaction_hd_revenue_to_practice
    , financials.quantile_75_hd_revenue_to_practice as quantile_75_transaction_hd_revenue_to_practice
    , financials.quantile_90_hd_revenue_to_practice as quantile_90_transaction_hd_revenue_to_practice

    , client_quantiles_total_revenue_per_practice.client_quantile_10_total_hd_revenue_to_practice as quantile_10_total_client_hd_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_25_total_hd_revenue_to_practice as quantile_25_total_client_hd_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_50_total_hd_revenue_to_practice as quantile_50_total_client_hd_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_75_total_hd_revenue_to_practice as quantile_75_total_client_hd_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_90_total_hd_revenue_to_practice as quantile_90_total_client_hd_revenue_to_practice_all_clients_at_clinic

    , client_quantiles_total_revenue_all_practices.client_quantile_10_total_hd_revenue_to_practice as quantile_10_total_client_hd_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_25_total_hd_revenue_to_practice as quantile_25_total_client_hd_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_50_total_hd_revenue_to_practice as quantile_50_total_client_hd_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_75_total_hd_revenue_to_practice as quantile_75_total_client_hd_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_90_total_hd_revenue_to_practice as quantile_90_total_client_hd_revenue_to_practice_all_clients_all_clinics

    , practices_financials.quantile_10_hd_revenue_to_practice as quantile_10_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_25_hd_revenue_to_practice as quantile_25_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_50_hd_revenue_to_practice as quantile_50_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_75_hd_revenue_to_practice as quantile_75_transaction_hd_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_90_hd_revenue_to_practice as quantile_90_transaction_hd_revenue_to_practice_all_clients_at_clinic

    , aggregate_financials_all_clients_all_practices.quantile_10_hd_revenue_to_practice as quantile_10_transaction_hd_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_25_hd_revenue_to_practice as quantile_25_transaction_hd_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_50_hd_revenue_to_practice as quantile_50_transaction_hd_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_75_hd_revenue_to_practice as quantile_75_transaction_hd_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_90_hd_revenue_to_practice as quantile_90_transaction_hd_revenue_to_practice_all_clients_all_clinics

    /* Revenue to Practice In-Clinic */
    , financials.total_inclinic_revenue_to_practice

    , financials.quantile_10_inclinic_revenue_to_practice as quantile_10_transaction_inclinic_revenue_to_practice
    , financials.quantile_25_inclinic_revenue_to_practice as quantile_25_transaction_inclinic_revenue_to_practice
    , financials.quantile_50_inclinic_revenue_to_practice as quantile_50_transaction_inclinic_revenue_to_practice
    , financials.quantile_75_inclinic_revenue_to_practice as quantile_75_transaction_inclinic_revenue_to_practice
    , financials.quantile_90_inclinic_revenue_to_practice as quantile_90_transaction_inclinic_revenue_to_practice

    , client_quantiles_total_revenue_per_practice.client_quantile_10_total_inclinic_revenue_to_practice as quantile_10_total_client_inclinic_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_25_total_inclinic_revenue_to_practice as quantile_25_total_client_inclinic_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_50_total_inclinic_revenue_to_practice as quantile_50_total_client_inclinic_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_75_total_inclinic_revenue_to_practice as quantile_75_total_client_inclinic_revenue_to_practice_all_clients_at_clinic
    , client_quantiles_total_revenue_per_practice.client_quantile_90_total_inclinic_revenue_to_practice as quantile_90_total_client_inclinic_revenue_to_practice_all_clients_at_clinic

    , client_quantiles_total_revenue_all_practices.client_quantile_10_total_inclinic_revenue_to_practice as quantile_10_total_client_inclinic_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_25_total_inclinic_revenue_to_practice as quantile_25_total_client_inclinic_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_50_total_inclinic_revenue_to_practice as quantile_50_total_client_inclinic_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_75_total_inclinic_revenue_to_practice as quantile_75_total_client_inclinic_revenue_to_practice_all_clients_all_clinics
    , client_quantiles_total_revenue_all_practices.client_quantile_90_total_inclinic_revenue_to_practice as quantile_90_total_client_inclinic_revenue_to_practice_all_clients_all_clinics

    , practices_financials.quantile_10_inclinic_revenue_to_practice as quantile_10_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_25_inclinic_revenue_to_practice as quantile_25_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_50_inclinic_revenue_to_practice as quantile_50_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_75_inclinic_revenue_to_practice as quantile_75_transaction_inclinic_revenue_to_practice_all_clients_at_clinic
    , practices_financials.quantile_90_inclinic_revenue_to_practice as quantile_90_transaction_inclinic_revenue_to_practice_all_clients_at_clinic

    , aggregate_financials_all_clients_all_practices.quantile_10_inclinic_revenue_to_practice as quantile_10_transaction_inclinic_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_25_inclinic_revenue_to_practice as quantile_25_transaction_inclinic_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_50_inclinic_revenue_to_practice as quantile_50_transaction_inclinic_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_75_inclinic_revenue_to_practice as quantile_75_transaction_inclinic_revenue_to_practice_all_clients_all_clinics
    , aggregate_financials_all_clients_all_practices.quantile_90_inclinic_revenue_to_practice as quantile_90_transaction_inclinic_revenue_to_practice_all_clients_all_clinics

    /* Wellness */
    , heartworm_current_status
    , internal_para_current_status
    , external_para_current_status
    , tick_pathogen_current_status
    , wellness_exam_current_status
    , dhpp_current_status
    , fvrcp_current_status
    , wellness_bloodwork_current_status
    , dental_prophy_current_status
    , total_medical_services_transactions
    , total_heartworm_compliant
    , total_heartworm_trx
    , percentage_heartworm_compliant
    , total_int_para_compliant
    , total_int_para_trx
    , percentage_int_para_compliant
    , total_dhpp_compliant
    , total_dhpp_trx
    , percentage_dhpp_compliant
    , total_fvrcp_compliant
    , total_fvrcp_trx
    , percentage_fvrcp_compliant
    , total_wellness_bloodwork_compliant
    , total_wellness_bloodwork_trx
    , percentage_wellness_bloodwork_compliant
    , total_dental_prophy_compliant
    , total_dental_prophy_trx
    , percentage_dental_prophy_compliant
    , total_tick_pathogen_compliant
    , total_tick_pathogen_trx
    , percentage_tick_pathogen_compliant
    , total_wellness_exam_compliant
    , total_wellness_exam_trx
    , percentage_wellness_exam_compliant
    , total_wellness_chances
    , total_wellness_compliant
    , master_wellness_compliance_score

from {{ ref('ifs_clients') }} as clients
left join {{ ref('ifs_clients_demographics') }} as demographics on clients.client_odu_id = demographics.client_odu_id
left join {{ ref('ifs_clients_autoship') }} as autoship on clients.client_odu_id = autoship.client_odu_id
left join {{ ref('ifs_clients_patients') }} as patients on clients.client_odu_id = patients.client_odu_id
left join {{ ref('ifs_clients_financials') }} as financials on clients.client_odu_id = financials.client_odu_id
left join {{ ref('ifs_clients_quantiles_total_revenue_per_practice') }} as client_quantiles_total_revenue_per_practice on clients.practice_odu_id = client_quantiles_total_revenue_per_practice.practice_odu_id
left join {{ ref('ifs_clients_quantiles_total_revenue_all_practices') }} as client_quantiles_total_revenue_all_practices on all_practices_id = client_quantiles_total_revenue_all_practices.finance_id
left join {{ ref('ifs_practices_financials') }} as practices_financials on clients.practice_odu_id = practices_financials.practice_odu_id
left join {{ ref('ifs_aggregate_financials_all_clients_all_practices') }} as aggregate_financials_all_clients_all_practices on all_practices_id = aggregate_financials_all_clients_all_practices.finance_id
left join {{ ref('ifs_clients_wellness') }} as clients_wellness on clients.client_odu_id = clients_wellness.client_odu_id
left join {{ ref('ifs_practices_patients_spays_and_neuters') }} as spay_and_neuter on clients.practice_odu_id = spay_and_neuter.practice_odu_id
left join {{ ref('ifs_clients_ecomm_website') }} as ecomm_website on clients.visitor_vso_id = ecomm_website.visitor_vso_id
left join {{ ref('ifs_clients_promotions') }} as promotions on clients.client_odu_id = promotions.client_odu_id
left join {{ ref('ifs_clients_petmail') }} as petmail on clients.client_odu_id = petmail.client_odu_id
