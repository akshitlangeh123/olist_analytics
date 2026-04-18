with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        count(order_item_id)            as total_items,
        sum(price)                      as total_item_value,
        sum(freight_value)              as total_freight_value,
        sum(price + freight_value)      as total_order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value)                          as total_payment_value,
        max(payment_installments)                   as max_installments,
        count(distinct payment_type)                as payment_methods_used,
        max(case when payment_sequential = 1
            then payment_type end)                  as primary_payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews as (
    select
        order_id,
        review_score,
        comment_message
    from (
        select
            order_id,
            review_score,
            comment_message,
            row_number() over (
                partition by order_id 
                order by review_created_at desc
            ) as rn
        from {{ ref('stg_order_reviews') }}
    ) ranked
    where rn = 1
),

final as (
    select
        -- keys
        o.order_id,
        o.customer_id,
        cast(date_format(o.purchased_at, 'yyyyMMdd') as int)    as date_key,

        -- status
        o.order_status,

        -- dates
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        date(o.purchased_at)                                     as purchase_date,
        year(o.purchased_at)                                     as purchase_year,
        month(o.purchased_at)                                    as purchase_month,
        quarter(o.purchased_at)                                  as purchase_quarter,
        dayofweek(o.purchased_at)                               as purchase_day_of_week,

        -- delivery metrics
        datediff(day, o.purchased_at, o.delivered_at)           as days_to_deliver,
        datediff(day, o.estimated_delivery_at, o.delivered_at)  as delivery_delay_days,
        case
            when o.delivered_at > o.estimated_delivery_at
            then true else false
        end                                                      as is_late_delivery,

        -- measures
        oi.total_items,
        oi.total_item_value,
        oi.total_freight_value,
        oi.total_order_value,
        p.total_payment_value,
        p.max_installments,
        p.payment_methods_used,
        p.primary_payment_type,

        -- review
        r.review_score,
        r.comment_message

    from orders o
    left join order_items oi    on o.order_id = oi.order_id
    left join payments p        on o.order_id = p.order_id
    left join reviews r         on o.order_id = r.order_id
)

select * from final