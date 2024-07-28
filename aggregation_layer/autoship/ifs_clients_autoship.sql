
select
    client_odu_id
    , boolor_agg(is_autoship_item_active) as is_autoship_active
    , sum(revenue_to_vso) as total_autoship_revenue_to_vso
from {{ ref('ifs_transactions_online_v2') }} as transactions
left join {{ ref('autoships') }} as autoship on transactions.autoship_line_id = autoship.autoship_line_id
where is_hd
    and is_vso_debit
    and is_autoship
group by 1
