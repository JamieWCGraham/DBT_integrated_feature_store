select
    clients.client_odu_id
    , income.zip
    , income.household_average_income as zipcode_household_average_income
    , clients_primary_contact.email_address is not null as is_email_available
    , clients_primary_contact.phone is not null as is_phone_available
from {{ ref('ifs_clients') }} as clients
left join {{ ref('clients_primary_contact') }} as clients_primary_contact on clients.client_odu_id = clients_primary_contact.client_odu_id
left join {{ ref('vat_uploads__us_zipcode_income') }} as income on clients_primary_contact.zip::varchar = income.zip::varchar
