with
source_product as (
    select * from {{ source('raw_sources','product') }}
)
select
    id as product_id,
    initcap(trim(name)) as product_name,
    initcap(trim(supplier_name)) as supplier_name,
    cast(created_at as timestamp) as created_at
from source_product
