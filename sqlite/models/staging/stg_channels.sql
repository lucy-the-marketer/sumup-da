{{ config(materialized='table') }}

with raw as (
    select
        {{ clean_id('CAMPAIGN_ID') }} as campaign_id,
        {{ normalize_text('CAMPAIGN_NAME') }} as campaign_name,
        {{ normalize_text('CAMPAIGN_PERIOD_BUDGET_CATEGORY') }} as campaign_period_budget_category,
        {{ normalize_text('CHANNEL_3') }} as channel_3,
        {{ normalize_text('CHANNEL_4') }} as channel_4,
        {{ normalize_text('CHANNEL_5') }} as channel_5
    from {{ ref('raw_channels') }}
),

cleaned as (
    select
        campaign_id,
        campaign_name,
        campaign_period_budget_category,
        channel_3,
        channel_4,

        -- Choose the most detailed `channel_5` value if duplicates exist
        coalesce(nullif(channel_5, ''), max(channel_5) over (
            partition by campaign_id, channel_3, channel_4
        )) as channel_5

    from raw
)

select distinct * from cleaned;
