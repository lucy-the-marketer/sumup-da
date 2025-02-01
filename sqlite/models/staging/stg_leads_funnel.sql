{{ config(
    materialized='incremental',
    unique_key='date'
) }}

with raw as (
    select
        strftime('%Y-%m-%d', DATE) as date,
        CURRENCY as currency,
        COUNTRY_CODE as country_code,
        {{ clean_id('CAMPAIGN_ID') }} as campaign_id,

        -- Store original values
        TOTAL_IMPRESSIONS as source_total_impressions,
        TOTAL_CLICKS as source_total_clicks,
        TOTAL_LEADS as source_total_leads,
        TOTAL_FAKE_LEADS as source_total_fake_leads,
        TOTAL_SQLS as source_total_sqls,
        TOTAL_MEETING_DONE as source_total_meeting_done,
        TOTAL_SIGNED_LEADS as source_total_signed_leads,
        TOTAL_POS_LITE_DEALS as source_total_pos_lite_deals,

        -- Apply outlier detection and replacement
        case 
            when TOTAL_IMPRESSIONS > (avg(TOTAL_IMPRESSIONS) over () * 1.5) 
                then round(avg(TOTAL_IMPRESSIONS) over ())
            else {{ intify('TOTAL_IMPRESSIONS') }}
        end as total_impressions,
        
        case 
            when TOTAL_CLICKS > (avg(TOTAL_CLICKS) over () * 1.5) 
                then round(avg(TOTAL_CLICKS) over ())
            else {{ intify('TOTAL_CLICKS') }}
        end as total_clicks,
        
        case 
            when TOTAL_LEADS > 100
                then 0
            else {{ intify('TOTAL_LEADS') }}
        end as total_leads,

        case 
            when TOTAL_FAKE_LEADS > 100
                then 0
            else {{ intify('TOTAL_FAKE_LEADS') }}
        end as total_fake_leads,

        case 
            when TOTAL_SQLS > 100
                then 0
            else {{ intify('TOTAL_SQLS') }}
        end as total_sqls,

        case 
            when TOTAL_MEETING_DONE > 100 
                then 0
            else {{ intify('TOTAL_MEETING_DONE') }}
        end as total_meeting_done,

        {{ intify('TOTAL_SIGNED_LEADS') }} as total_signed_leads,

        {{ intify('TOTAL_POS_LITE_DEALS') }} as total_pos_lite_deals,

        cast(TOTAL_SPEND as DECIMAL) as total_spend_eur
    from {{ ref('raw_leads_funnel') }}
)

select
    raw.*,
    (raw.total_leads - raw.total_fake_leads) as valid_leads
from raw

