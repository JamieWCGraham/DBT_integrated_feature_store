select
    transactions_online.practice_odu_id
    , practice_online_details.old_owner_first_practice_hd_purchase_date as first_hd_purchase_date
    , datediff(month,first_hd_purchase_date,current_date) <= 6 as is_first_hd_purchase_last_6_months
    , datediff(month,first_hd_purchase_date,current_date) <= 3 as is_first_hd_purchase_last_3_months
    , datediff(month,first_hd_purchase_date,current_date) <= 12 as is_first_hd_purchase_last_12_months
from {{ ref('transactions_online_v2') }} as transactions_online
inner join {{ ref('practice_online_details') }} as practice_online_details on transactions_online.practice_odu_id = practice_online_details.practice_odu_id
left join {{ ref('practices') }} as practices on transactions_online.practice_odu_id = practices.practice_odu_id
left join {{ ref('dates') }} as dates on transactions_online.reporting_date  = dates.date
where pims_extraction_completness in ('complete')
    and transactions_online.is_hd
    and dates.is_last_24_complete_months
    -- and practice_online_details.site_vso_id in (170319,168392,163571) /* temporary clause */
    -- and practice_online_details.practice_odu_id in ('370-multi','371-multi')
group by all
