with inclinic_patients as (
    select
        transactions_inclinic.patient_odu_id
        , transactions_inclinic.client_odu_id
        , true as is_inclinic_patient
    from {{ ref('ifs_transactions_inclinic') }} as transactions_inclinic
    group by 1,2
)

, online_patients as (
    select
        transactions_online.patient_odu_id
        , transactions_online.client_odu_id
        , count(distinct case when is_hd then transactions_online.transaction_odu_id end) > 0 as is_hd_patient
        , count(distinct case when is_econnect then transactions_online.transaction_odu_id end) > 0 as is_econnect_patient
    from {{ ref('ifs_transactions_online_v2') }} as transactions_online
    group by 1,2
)

select
    coalesce(inclinic_patients.patient_odu_id,online_patients.patient_odu_id) as patient_odu_id
    , coalesce(inclinic_patients.client_odu_id,online_patients.client_odu_id) as client_odu_id
    , ifnull(online_patients.is_hd_patient,false) as is_hd_patient
    , ifnull(online_patients.is_econnect_patient,false) as is_econnect_patient
    , ifnull(inclinic_patients.is_inclinic_patient,false) as is_inclinic_patient
    , patients.species
    , patients.age
from inclinic_patients
full outer join online_patients on inclinic_patients.client_odu_id = online_patients.client_odu_id
    and inclinic_patients.patient_odu_id = online_patients.patient_odu_id
inner join {{ ref('patients') }} as patients on coalesce(inclinic_patients.patient_odu_id,online_patients.patient_odu_id) = patients.patient_odu_id
