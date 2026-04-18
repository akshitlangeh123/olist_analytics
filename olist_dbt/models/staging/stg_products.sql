with source as (
    select * from {{ source('bronze', 'products') }}
),

renamed as (
    select
        product_id,
        product_category_name                        as category_name_pt,
        cast(product_name_lenght as int)             as product_name_length,
        cast(product_description_lenght as int)      as product_description_length,
        cast(product_photos_qty as int)              as photos_qty,
        cast(product_weight_g as decimal(10,2))      as weight_g,
        cast(product_length_cm as decimal(10,2))     as length_cm,
        cast(product_height_cm as decimal(10,2))     as height_cm,
        cast(product_width_cm as decimal(10,2))      as width_cm
    from source
)

select * from renamed