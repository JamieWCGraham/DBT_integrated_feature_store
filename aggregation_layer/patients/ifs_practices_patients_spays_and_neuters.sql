
select
    transactions.practice_odu_id
    , avg(reporting_amount) as clinic_average_spay_and_neuter_cost
from {{ ref('ifs_transactions_inclinic') }} as transactions
where transactions.level_2_revenue_category_name = 'Spays and Neuters'
group by 1

