with source as (
    select * from {{ source('bronze', 'order_reviews') }}
),

renamed as (
    select
        review_id,
        order_id,
        cast(review_score as int)                    as review_score,
        review_comment_title                         as comment_title,
        review_comment_message                       as comment_message,
        cast(review_creation_date as timestamp)      as review_created_at,
        cast(review_answer_timestamp as timestamp)   as review_answered_at
    from source
),

cleaned as (
    select * from renamed
    where review_id is not null
      and order_id is not null
      and review_score is not null
      and review_score between 1 and 5
)

select * from cleaned