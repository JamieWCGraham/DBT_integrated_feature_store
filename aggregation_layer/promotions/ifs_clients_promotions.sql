select
    transactions.client_odu_id
    , count(promotions.transaction_odu_id) > 0 as has_promo_usage
from {{ ref('ifs_transactions_online_v2') }} as transactions
left join {{ ref('promotion_transactions') }} as promotions on transactions.transaction_odu_id = promotions.transaction_odu_id
where promotions.promotion_name is not null
group by 1
