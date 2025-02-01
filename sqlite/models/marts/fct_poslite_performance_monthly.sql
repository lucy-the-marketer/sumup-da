{{ config(
    materialized='table'
) }}

with web_orders as (
    select 
        strftime('%Y-%m', date) as month,
        country_code,
        campaign_id,
        sum(total_spend_eur) as web_spend,
        sum(nb_of_sessions) as total_sessions,
        sum(nb_of_signups) as total_signups,
        sum(nb_of_orders) as total_orders,
        sum(nb_of_valid_order) as total_valid_orders,
        sum(nb_of_poslite_items_ordered) as total_items_ordered,
        sum(nb_poslite_items_dispatched) as total_items_dispatched
    from {{ ref('stg_web_orders') }}
    group by 1,2,3
),

leads_funnel as (
    select 
        strftime('%Y-%m', date) as month,
        country_code,
        campaign_id,
        sum(total_spend_eur) as leads_spend,
        sum(total_impressions) as total_impressions,
        sum(total_clicks) as total_clicks,
        sum(total_leads) as total_leads,
        sum(valid_leads) as total_valid_leads,
        sum(total_sqls) as total_sqls,
        sum(total_meeting_done) as total_meetings,
        sum(total_signed_leads) as total_signed,
        sum(total_pos_lite_deals) as total_deals
    from {{ ref('stg_leads_funnel') }}
    group by 1,2,3
),

combined as (
    select 
        coalesce(w.month, l.month) as month,
        coalesce(w.country_code, l.country_code) as country_code,
        coalesce(w.campaign_id, l.campaign_id) as campaign_id,

        -- Marketing spend
        coalesce(w.web_spend, 0) + coalesce(l.leads_spend, 0) as total_spend_eur,
        coalesce(w.web_spend, 0) as web_spend_eur,
        coalesce(l.leads_spend, 0) as leads_spend_eur,

        -- Marketing performance
        coalesce(w.total_sessions, 0) as total_sessions,
        coalesce(w.total_signups, 0) as total_signups,
        coalesce(l.total_impressions, 0) as total_impressions,
        coalesce(l.total_clicks, 0) as total_clicks,

        -- Orders and Leads
        coalesce(w.total_orders, 0) as total_orders,
        coalesce(w.total_valid_orders, 0) as total_valid_orders,
        coalesce(l.total_leads, 0) as total_leads,
        coalesce(l.total_valid_leads, 0) as total_valid_leads,
        coalesce(l.total_sqls, 0) as total_sqls,
        coalesce(l.total_meetings, 0) as total_meetings,
        coalesce(l.total_signed, 0) as total_signed,
        coalesce(l.total_deals, 0) as total_deals,

        coalesce(w.total_valid_orders, 0) + coalesce(l.total_deals, 0) as total_item_sold,

        -- Conversion Rates
        case 
            when total_impressions > 0 then total_clicks * 1.0 / total_impressions 
            else 0 
        end as click_through_rate,

        case 
            when total_clicks > 0 then web_spend / total_clicks 
            else 0 
        end as cost_per_click,

        case 
            when total_leads > 0 then leads_spend / total_leads 
            else 0 
        end as cost_per_lead,

        case 
            when total_leads > 0 then (web_spend + leads_spend) / total_leads 
            else 0 
        end as blended_cost_per_lead,

        case 
            when total_orders > 0 then leads_spend / total_orders 
            else 0 
        end as cost_per_order,

        case 
            when total_orders > 0 then (web_spend + leads_spend) / total_orders 
            else 0 
        end as blended_cost_per_order,

        -- Additional Conversion Rates
        case 
            when total_sessions > 0 then total_signups * 1.0 / total_sessions 
            else 0 
        end as session_to_signup_rate,
        
        case 
            when total_signups > 0 then total_orders * 1.0 / total_signups 
            else 0 
        end as signup_to_order_rate,
        
        case 
            when total_clicks > 0 then total_leads * 1.0 / total_clicks 
            else 0 
        end as lead_conversion_rate,
        
        case 
            when total_leads > 0 then total_sqls * 1.0 / total_leads 
            else 0 
        end as sql_conversion_rate,
        
        case 
            when total_sqls > 0 then total_meetings * 1.0 / total_sqls 
            else 0 
        end as meeting_conversion_rate,
        
        case 
            when total_meetings > 0 then total_signed * 1.0 / total_meetings 
            else 0 
        end as signed_conversion_rate,
        
        case 
            when total_signed > 0 then total_orders * 1.0 / total_signed 
            else 0 
        end as order_conversion_rate

    from web_orders w
    full outer join leads_funnel l
      on w.month = l.month
     and w.country_code = l.country_code
     and w.campaign_id = l.campaign_id
)

select * from combined;