{{ config(materialized='table') }}

with base as (
    select distinct
        campaign_id,
        channel_3,
        channel_4,
        channel_5
    from {{ ref('stg_channels') }}
),

dim as (
    select
        row_number() over (order by campaign_id) as channel_key,
        campaign_id,
        channel_3,
        channel_4,
        channel_5
    from base
)

select *
from dim
