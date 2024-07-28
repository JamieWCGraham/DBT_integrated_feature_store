with inclinic_clients as (
    select
        transactions_inclinic.client_odu_id
        , min(transactions_inclinic.practice_odu_id) as practice_odu_id
        , max(reporting_date) as max_reporting_date
        , true as is_inclinic_client
        , count(distinct practice_odu_id) = 1 as is_single_practice_client
    from {{ ref('ifs_transactions_inclinic') }} as transactions_inclinic
    group by all
    having is_single_practice_client
)

, online_clients as (
    select
        transactions_online.client_odu_id
        , min(transactions_online.practice_odu_id) as practice_odu_id
        , max(reporting_date) as max_reporting_date
        , count(distinct case when is_hd then transactions_online.transaction_odu_id end) > 0 as is_hd_client
        , count(distinct case when is_econnect then transactions_online.transaction_odu_id end) > 0 as is_econnect_client
        , count(distinct practice_odu_id) = 1 as is_single_practice_client
    from {{ ref('ifs_transactions_online_v2') }} as transactions_online
    group by all
    having is_single_practice_client
)

select
    coalesce(inclinic_clients.client_odu_id,online_clients.client_odu_id) as client_odu_id
    , coalesce(inclinic_clients.practice_odu_id,online_clients.practice_odu_id) as practice_odu_id
    , ifnull(online_clients.is_hd_client,false) as is_hd_client
    , ifnull(online_clients.is_econnect_client,false) as is_econnect_client
    , ifnull(inclinic_clients.is_inclinic_client,false) as is_inclinic_client
    , clients.is_safe_to_contact
    , clients.visitor_vso_id
from inclinic_clients
full outer join online_clients on inclinic_clients.client_odu_id = online_clients.client_odu_id
inner join {{ ref('clients') }} as clients on coalesce(inclinic_clients.client_odu_id,online_clients.client_odu_id) = clients.client_odu_id
