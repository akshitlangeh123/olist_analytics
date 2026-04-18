with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
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
        -- surrogate key at order-item grain
        concat(oi.order_id, '_', oi.order_item_id)          as order_item_key,

        -- foreign keys
        oi.order_id,
        oi.product_id                                        as product_key,
        oi.seller_id                                         as seller_key,
        o.customer_id,
        cast(date_format(o.purchased_at, 'yyyyMMdd') as int) as date_key,

        -- order item measures
        oi.order_item_id,
        oi.price,
        oi.freight_value,
        oi.price + oi.freight_value                          as item_total_value,

        -- order status & dates
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        date(o.purchased_at)                                 as purchase_date,
        year(o.purchased_at)                                 as purchase_year,
        month(o.purchased_at)                                as purchase_month,
        quarter(o.purchased_at)                              as purchase_quarter,
        dayofweek(o.purchased_at)                           as purchase_day_of_week,

        -- delivery metrics
        datediff(day, o.purchased_at, o.delivered_at)        as days_to_deliver,
        datediff(day, o.estimated_delivery_at, o.delivered_at) as delivery_delay_days,
        case
            when o.delivered_at > o.estimated_delivery_at
            then true else false
        end                                                  as is_late_delivery,

        -- order-level payment (repeated across items, use carefully in DAX)
        p.total_payment_value,
        p.max_installments,
        p.payment_methods_used,
        p.primary_payment_type,

        -- review
        r.review_score,
        r.comment_message

    from order_items oi
    left join orders o       on oi.order_id = o.order_id
    left join payments p     on oi.order_id = p.order_id
    left join reviews r      on oi.order_id = r.order_id
)

select * from final