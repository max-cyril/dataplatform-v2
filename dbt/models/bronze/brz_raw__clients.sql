with
source_clients as (
    select * from {{ source('raw_sources','clients') }}
)
select
    id as client_id,
    initcap(trim(name)) as client_name,
    lower(trim(email)) as client_email,
    upper(trim(country)) as country,
    cast(created_at as timestamp) as created_at
from source_clients
