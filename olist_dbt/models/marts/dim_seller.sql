with sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        order_status,
        review_score,
        is_late_delivery,
        days_to_deliver
    from {{ ref('fact_orders') }}
),

seller_stats as (
    select
        oi.seller_id,
        count(distinct oi.order_id)                         as total_orders,
        count(oi.order_item_id)                             as total_units_sold,
        sum(oi.price)                                       as total_revenue,
        avg(oi.price)                                       as avg_item_price,
        avg(o.review_score)                                 as avg_review_score,
        avg(o.days_to_deliver)                              as avg_days_to_deliver,
        sum(case when o.is_late_delivery
            then 1 else 0 end)                              as late_deliveries,
        sum(case when o.order_status = 'canceled'
            then 1 else 0 end)                              as canceled_orders
    from order_items oi
    left join orders o on oi.order_id = o.order_id
    group by oi.seller_id
),

final as (
    select
        s.seller_id                                         as seller_key,
        s.seller_id,
        s.city,
        s.state,
        s.zip_code,

        -- performance
        ss.total_orders,
        ss.total_units_sold,
        ss.total_revenue,
        ss.avg_item_price,
        ss.avg_review_score,
        ss.avg_days_to_deliver,
        ss.late_deliveries,
        ss.canceled_orders,
        round(ss.late_deliveries / nullif(ss.total_orders, 0) * 100, 2) as late_delivery_rate_pct,

        -- seller tier based on revenue
        case
            when ss.total_revenue >= 50000  then 'Platinum'
            when ss.total_revenue >= 20000  then 'Gold'
            when ss.total_revenue >= 5000   then 'Silver'
            else 'Bronze'
        end as seller_tier
    from sellers s
    left join seller_stats ss on s.seller_id = ss.seller_id
)

select * from final