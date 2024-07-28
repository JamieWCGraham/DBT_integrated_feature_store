{{
    config(
        unique_key = 'client_odu_id'
        , snowflake_warehouse=get_warehouse('xlarge')
    )
}}

with last_medical_service_checks as (
    select
        client_odu_id
        , heartworm_test_compliance_status as heartworm_current_status
        , internal_para_compliance_status as internal_para_current_status
        , external_para_compliance_status as external_para_current_status
        , tick_pathogen_compliance_status as tick_pathogen_current_status
        , wellness_exam_compliance_status as wellness_exam_current_status
        , dhpp_compliance_status as dhpp_current_status
        , fvrcp_compliance_status as fvrcp_current_status
        , wellness_bloodwork_compliance_status as wellness_bloodwork_current_status
        , dental_prophy_compliance_status as dental_prophy_current_status
    from {{ ref('ifs_transactions') }} as transactions
    left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
    where transactions.is_medical_service
    and transactions.is_inclinic
    and reporting_date is not null
    qualify row_number() over (partition by client_odu_id order by reporting_date desc) = 1
)

select
    transactions.client_odu_id
    , last_medical_service_checks.heartworm_current_status
    , last_medical_service_checks.internal_para_current_status
    , last_medical_service_checks.external_para_current_status
    , last_medical_service_checks.tick_pathogen_current_status
    , last_medical_service_checks.wellness_exam_current_status
    , last_medical_service_checks.dhpp_current_status
    , last_medical_service_checks.fvrcp_current_status
    , last_medical_service_checks.wellness_bloodwork_current_status
    , last_medical_service_checks.dental_prophy_current_status

    /* Historical Wellness */
    , count(transactions.transaction_odu_id) as total_medical_services_transactions
    , count(distinct case when transactions.heartworm_test_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_heartworm_compliant
    , count(distinct case when (transactions.heartworm_test_compliance_status is not null and transactions.heartworm_test_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_heartworm_trx
    , div0(total_heartworm_compliant,total_heartworm_trx) as percentage_heartworm_compliant
    , count(distinct case when transactions.internal_para_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_int_para_compliant
    , count(distinct case when (transactions.internal_para_compliance_status is not null and transactions.internal_para_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_int_para_trx
    , div0(total_int_para_compliant,total_int_para_trx) as percentage_int_para_compliant
    , count(distinct case when transactions.external_para_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_ext_para_compliant
    , count(distinct case when (transactions.external_para_compliance_status is not null and transactions.external_para_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_ext_para_trx
    , div0(total_ext_para_compliant,total_ext_para_trx) as percentage_ext_para_compliant
    , count(distinct case when transactions.dhpp_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_dhpp_compliant
    , count(distinct case when (transactions.dhpp_compliance_status is not null and transactions.dhpp_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_dhpp_trx
    , div0(total_dhpp_compliant,total_dhpp_trx) as percentage_dhpp_compliant
    , count(distinct case when transactions.fvrcp_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_fvrcp_compliant
    , count(distinct case when (transactions.fvrcp_compliance_status is not null and transactions.fvrcp_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_fvrcp_trx
    , div0(total_fvrcp_compliant,total_fvrcp_trx) as percentage_fvrcp_compliant
    , count(distinct case when transactions.wellness_bloodwork_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_wellness_bloodwork_compliant
    , count(distinct case when (transactions.wellness_bloodwork_compliance_status is not null and transactions.wellness_bloodwork_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_wellness_bloodwork_trx
    , div0(total_wellness_bloodwork_compliant,total_wellness_bloodwork_trx) as percentage_wellness_bloodwork_compliant
    , count(distinct case when transactions.dental_prophy_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_dental_prophy_compliant
    , count(distinct case when (transactions.dental_prophy_compliance_status is not null and transactions.dental_prophy_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_dental_prophy_trx
    , div0(total_dental_prophy_compliant,total_dental_prophy_trx) as percentage_dental_prophy_compliant
    , count(distinct case when transactions.tick_pathogen_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_tick_pathogen_compliant
    , count(distinct case when (transactions.tick_pathogen_compliance_status is not null and transactions.tick_pathogen_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_tick_pathogen_trx
    , div0(total_tick_pathogen_compliant,total_tick_pathogen_trx) as percentage_tick_pathogen_compliant
    , count(distinct case when transactions.wellness_exam_compliance_status = 'Compliant' then transactions.transaction_odu_id else null end) AS total_wellness_exam_compliant
    , count(distinct case when (transactions.wellness_exam_compliance_status is not null and transactions.wellness_exam_compliance_status <> 'N/A') then transactions.transaction_odu_id else null end) AS total_wellness_exam_trx
    , div0(total_wellness_exam_compliant,total_wellness_exam_trx) as percentage_wellness_exam_compliant

    , total_heartworm_trx + total_int_para_trx + total_ext_para_trx + total_tick_pathogen_trx + total_wellness_exam_trx + total_dhpp_trx + total_fvrcp_trx + total_wellness_bloodwork_trx + total_dental_prophy_trx as total_wellness_chances
    , total_heartworm_compliant + total_int_para_compliant + total_ext_para_compliant + total_tick_pathogen_compliant + total_wellness_exam_compliant + total_dhpp_compliant + total_fvrcp_compliant + total_wellness_bloodwork_compliant + total_dental_prophy_compliant as total_wellness_compliant
    , div0(total_wellness_compliant, total_wellness_chances) as master_wellness_compliance_score

from {{ ref('ifs_transactions') }} as transactions
left join last_medical_service_checks on transactions.client_odu_id = last_medical_service_checks.client_odu_id
left join {{ ref('dates') }} as dates on transactions.reporting_date = dates.date
where transactions.is_medical_service
and transactions.is_inclinic
and reporting_date is not null
group by all
