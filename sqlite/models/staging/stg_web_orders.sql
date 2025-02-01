{{ config(
    materialized='incremental',
    unique_key='date'
) }}

with inferred_data as (
    select
        campaign_id,
        coalesce(strftime('%Y-%m-%d', max(date)), '1970-01-01') as inferred_date,  -- Ensure valid date format
        coalesce(max(country_code), 'unknown') as inferred_country_code
    from {{ ref('stg_leads_funnel') }}
    group by campaign_id
),

raw as (
    select
        -- Infer missing date and country_code from leads_funnel
        coalesce(
            case when typeof(w.date) = 'text' then strftime('%Y-%m-%d', w.date) else null end,
            case when typeof(i.inferred_date) = 'text' then strftime('%Y-%m-%d', i.inferred_date) else null end,
            '1970-01-01'
        ) as date,
        coalesce(nullif(trim(w.country_code), ''), i.inferred_country_code, 'unknown') as country_code,
        {{ clean_id('w.campaign_id') }} as campaign_id,
        cast(w.total_spend_eur as decimal) as total_spend_eur,

        -- Store original values
        w.nb_of_sessions as source_nb_of_sessions,
        w.nb_of_signups as source_nb_of_signups,
        w.nb_of_orders as source_nb_of_orders,
        w.nb_of_poslite_items_ordered as source_nb_of_poslite_items_ordered,
        w.nb_poslite_items_dispatched as source_nb_poslite_items_dispatched,

        -- Apply outlier detection and replacement
        case 
            when w.nb_of_sessions > (avg(w.nb_of_sessions) over () * 1.5) 
                then round(avg(w.nb_of_sessions) over ())
            else {{ intify('w.nb_of_sessions') }}
        end as nb_of_sessions,
        
        case 
            when w.nb_of_signups > (avg(w.nb_of_signups) over () * 1.5) 
                then round(avg(w.nb_of_signups) over ())
            else {{ intify('w.nb_of_signups') }}
        end as nb_of_signups,
        
        case 
            when w.nb_of_orders > 100 
                then 0
            else {{ intify('w.nb_of_orders') }}
        end as nb_of_orders,

        case 
            when w.nb_of_poslite_items_ordered > 100 
                then 0
            else {{ intify('w.nb_of_poslite_items_ordered') }}
        end as nb_of_poslite_items_ordered,
        
        {{ intify('w.nb_poslite_items_dispatched') }} as nb_poslite_items_dispatched,
        
        -- Binary flags for outliers
        case 
            when w.nb_of_sessions > (avg(w.nb_of_sessions) over () * 1.5) then 1 
            else 0
        end as is_outlier_nb_of_sessions,
        
        case 
            when w.nb_of_signups > (avg(w.nb_of_signups) over () * 1.5) then 1 
            else 0
        end as is_outlier_nb_of_signups,
        
        case 
            when w.nb_of_orders > (avg(w.nb_of_orders) over () * 1.5) then 1 
            else 0
        end as is_outlier_nb_of_orders,
        
        case 
            when w.nb_of_poslite_items_ordered > (avg(w.nb_of_poslite_items_ordered) over () * 1.5) then 1 
            else 0
        end as is_outlier_nb_of_poslite_items_ordered,
        
        case 
            when w.nb_poslite_items_dispatched > (avg(w.nb_poslite_items_dispatched) over () * 1.5) then 1 
            else 0
        end as is_outlier_nb_poslite_items_dispatched,
        
        -- Valid order condition
        case 
            when {{ intify('w.nb_of_orders') }} >= 1 
                 and {{ intify('w.nb_of_poslite_items_ordered') }} >= {{ intify('w.nb_of_orders') }} 
            then w.nb_of_orders 
            else 0 
        end as nb_of_valid_order
    from {{ ref('raw_web_orders') }} w
    left join inferred_data i
      on w.campaign_id = i.campaign_id
)
select * from raw;
