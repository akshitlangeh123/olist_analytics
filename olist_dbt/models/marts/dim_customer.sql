with customers as (
    select
        customer_unique_id,
        city,
        state,
        zip_code
    from (
        select
            customer_unique_id,
            city,
            state,
            zip_code,
            row_number() over (
                partition by customer_unique_id
                order by customer_id desc
            ) as rn
        from {{ ref('stg_customers') }}
    ) ranked
    where rn = 1
),

orders as (
    select
        c.customer_unique_id,
        count(distinct f.order_id)                          as total_orders,
        sum(f.total_payment_value)                          as total_spend,
        avg(f.total_payment_value)                          as avg_order_value,
        min(f.purchased_at)                                 as first_order_at,
        max(f.purchased_at)                                 as last_order_at,
        avg(f.review_score)                                 as avg_review_score,
        sum(case when f.is_late_delivery then 1 else 0 end) as late_deliveries,
        sum(case when f.order_status = 'canceled'
            then 1 else 0 end)                              as canceled_orders
    from {{ ref('fact_orders') }} f
    left join {{ ref('stg_customers') }} c on f.customer_id = c.customer_id
    group by c.customer_unique_id
),

rfm as (
    select
        customer_unique_id,
        total_orders,
        total_spend,
        avg_order_value,
        avg_review_score,
        late_deliveries,
        canceled_orders,
        first_order_at,
        last_order_at,
        datediff(day, last_order_at, current_timestamp())   as recency_days,
        datediff(day, first_order_at, last_order_at)        as customer_lifespan_days,
        ntile(5) over (order by datediff(day, last_order_at, current_timestamp()) desc) as recency_score,
        ntile(5) over (order by total_orders asc)           as frequency_score,
        ntile(5) over (order by total_spend asc)            as monetary_score
    from orders
),

rfm_segmented as (
    select
        *,
        recency_score + frequency_score + monetary_score    as rfm_total,
        case
            when recency_score >= 4 and frequency_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3 then 'Loyal'
            when recency_score >= 4 and frequency_score <= 2 then 'New customers'
            when recency_score <= 2 and frequency_score >= 3 then 'At risk'
            when recency_score <= 2 and frequency_score <= 2 then 'Lost'
            else 'Potential'
        end as rfm_segment
    from rfm
),

final as (
    select
        r.customer_unique_id                                as customer_key,
        r.customer_unique_id,
        c.city,
        c.state,
        c.zip_code,
        r.total_orders,
        r.total_spend,
        r.avg_order_value,
        r.avg_review_score,
        r.late_deliveries,
        r.canceled_orders,
        r.first_order_at,
        r.last_order_at,
        r.recency_days,
        r.customer_lifespan_days,
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total,
        r.rfm_segment
    from rfm_segmented r
    left join customers c on r.customer_unique_id = c.customer_unique_id
)

select * from final