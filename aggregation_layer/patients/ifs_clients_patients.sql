select
    clients.client_odu_id
    , count(distinct patients.patient_odu_id) as total_pets
    , min(patients.age) as minimum_pet_age
    , max(patients.age) as maximum_pet_age
    , round(avg(patients.age),2) as average_pet_age
    , max(case when patients.species = 'canine' then 1 else 0 end ) as has_dog
    , max(case when patients.species = 'feline' then 1 else 0 end ) as has_cat
    , listagg(distinct patients.species,',') as species
from {{ ref('ifs_patients') }} as patients
inner join {{ ref('ifs_clients') }} as clients on patients.client_odu_id = clients.client_odu_id
group by 1
