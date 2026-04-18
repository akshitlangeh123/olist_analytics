with products as (
    select * from {{ ref('stg_products') }}
),

translation as (
    select * from {{ ref('stg_product_category_translation') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        order_status,
        review_score,
        is_late_delivery
    from {{ ref('fact_orders') }}
),

product_stats as (
    select
        oi.product_id,
        count(oi.order_item_id)                             as total_units_sold,
        count(distinct oi.order_id)                         as total_orders,
        sum(oi.price)                                       as total_revenue,
        avg(oi.price)                                       as avg_price,
        sum(oi.freight_value)                               as total_freight,
        avg(oi.freight_value)                               as avg_freight,
        avg(o.review_score)                                 as avg_review_score,
        sum(case when o.order_status = 'canceled'
            then 1 else 0 end)                              as canceled_orders,
        sum(case when o.is_late_delivery
            then 1 else 0 end)                              as late_deliveries
    from order_items oi
    left join orders o on oi.order_id = o.order_id
    group by oi.product_id
),

final as (
    select
        p.product_id                                        as product_key,
        p.product_id,
        p.category_name_pt,
        coalesce(t.category_name_en, p.category_name_pt)   as category_name_en,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.photos_qty,

        -- sales performance
        ps.total_units_sold,
        ps.total_orders,
        ps.total_revenue,
        ps.avg_price,
        ps.total_freight,
        ps.avg_freight,

        -- freight efficiency
        round(ps.total_freight / nullif(ps.total_revenue, 0) * 100, 2) as freight_pct_of_revenue,

        -- quality signals
        ps.avg_review_score,
        ps.canceled_orders,
        ps.late_deliveries,
        round(ps.canceled_orders / nullif(ps.total_orders, 0) * 100, 2) as cancellation_rate_pct,
        round(ps.late_deliveries / nullif(ps.total_orders, 0) * 100, 2) as late_delivery_rate_pct
    from products p
    left join translation t     on p.category_name_pt = t.category_name_pt
    left join product_stats ps  on p.product_id = ps.product_id
)

select * from final