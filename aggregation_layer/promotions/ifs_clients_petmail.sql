select
    clients.client_odu_id
    , sum(promotion_transactions.mktg_adjusted_revenue) as total_revenue_through_petmail
    , total_revenue_through_petmail > 0 as has_petmail_generated_revenue
    , count(distinct promotion_transactions.transaction_odu_id) as total_transactions_through_petmail
    , count(distinct case when dates.is_last_12_complete_months then promotion_transactions.transaction_odu_id end) as total_transactions_through_petmail_last_12_months
    , ifnull(sum( case when dates.is_last_12_complete_months then promotion_transactions.mktg_adjusted_revenue end ),0) as total_revenue_through_petmail_last_12_months
from  {{ ref('promotion_transactions') }} as promotion_transactions
inner join {{ ref('ifs_clients') }} as clients on promotion_transactions.client_odu_id = clients.client_odu_id
inner join {{ ref('dates') }} on promotion_transactions.reporting_date::date = dates.date
where is_marketing_revenue
and promotion_category = 'PetMail'
group by 1
