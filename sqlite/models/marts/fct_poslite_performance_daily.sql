{{ config(materialized='table') }}

with web as (
    select
        date,
        country_code,
        campaign_id,
        sum(nb_of_sessions) as nb_of_sessions,
        sum(nb_of_signups) as nb_of_signups,
        sum(case when nb_of_valid_order = 1 then nb_of_orders else 0 end) as nb_of_orders,
        sum(case when nb_of_valid_order = 1 then nb_of_poslite_items_ordered else 0 end) as nb_of_poslite_items_ordered,
        sum(total_spend_eur) as web_spend
    from {{ ref('stg_web_orders') }}
    group by 1,2,3
),

leads as (
    select
        date,
        country_code,
        campaign_id,
        sum(total_impressions) as total_impressions,
        sum(total_clicks) as total_clicks,
        sum(total_spend_eur) as leads_spend,
        
        sum(total_leads) as total_leads,
        sum(total_fake_leads) as total_fake_leads,
        sum(valid_leads) as valid_leads,  
        sum(total_sqls) as total_sqls,
        sum(total_meeting_done) as total_meeting_done,
        sum(total_signed_leads) as total_signed_leads,
        sum(total_pos_lite_deals) as total_pos_lite_deals
    from {{ ref('stg_leads_funnel') }}
    group by 1,2,3
),

combined as (
    select
        coalesce(w.date, l.date) as date,
        coalesce(w.country_code, l.country_code) as country_code,
        coalesce(w.campaign_id, l.campaign_id) as campaign_id,

        -- Web
        w.nb_of_sessions,
        w.nb_of_signups,
        w.nb_of_orders,
        w.nb_of_poslite_items_ordered,
        w.web_spend,

        -- Leads
        l.total_impressions,
        l.total_clicks,
        l.leads_spend,
        l.total_leads,
        l.total_fake_leads,
        l.valid_leads,
        l.total_sqls,
        l.total_meeting_done,
        l.total_signed_leads,
        l.total_pos_lite_deals
    from web w
    full outer join leads l
      on w.date = l.date
     and w.country_code = l.country_code
     and w.campaign_id = l.campaign_id
),

joined_channels as (
    select
        c.*,
        d.channel_key,
        d.channel_3,
        d.channel_4,
        d.channel_5
    from combined c
    left join {{ ref('dim_channels') }} d
      on c.campaign_id = d.campaign_id
),

final as (
    select
        date,
        country_code,
        campaign_id,
        
        channel_key,
        channel_3,
        channel_4,
        
        /* --- Marketing Performance --- */
        coalesce(total_impressions, 0) as total_impressions,
        coalesce(total_clicks, 0) as total_clicks,
        (case when total_impressions>0 then total_clicks*1.0/total_impressions else 0 end) as ctr,
        
        (case when total_clicks>0
              then (coalesce(web_spend,0)+coalesce(leads_spend,0))/total_clicks
              else 0 end) as cpc,
        
        (case when valid_leads>0
              then (coalesce(web_spend,0)+coalesce(leads_spend,0))/valid_leads
              else 0 end) as cpl,

        /* --- Web Funnel --- */
        coalesce(nb_of_sessions, 0) as nb_of_sessions,
        coalesce(nb_of_signups, 0) as nb_of_signups,
        coalesce(nb_of_orders, 0) as nb_of_orders,
        coalesce(nb_of_poslite_items_ordered, 0) as nb_of_poslite_items_ordered,
        coalesce(web_spend, 0) as web_spend,
        
        /* --- Leads Funnel --- */
        coalesce(valid_leads, 0) as total_valid_leads,
        coalesce(total_sqls, 0) as total_sqls,
        coalesce(total_meeting_done, 0) as total_meeting_done,
        coalesce(total_signed_leads, 0) as total_signed_leads,
        coalesce(total_pos_lite_deals, 0) as total_pos_lite_deals,
        coalesce(leads_spend, 0) as leads_spend,

        /* --- Total --- */
        (COALESCE(nb_of_orders, 0) + COALESCE(total_pos_lite_deals, 0)) as total_orders,
        (COALESCE(web_spend, 0) + COALESCE(leads_spend, 0)) as total_spend

    from joined_channels
)

select *
from final
