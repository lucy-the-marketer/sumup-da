SELECT 
    campaign_id, 
    date, 
    country_code,
    total_impressions, 
    total_clicks, 
    total_leads, 
    total_fake_leads, 
    total_sqls, 
    total_meeting_done, 
    total_signed_leads, 
    total_pos_lite_deals,
    CASE 
        WHEN total_clicks > total_impressions 
            THEN 'Error: Total Clicks cannot exceed Total Impressions'
        WHEN total_fake_leads > total_leads 
            THEN 'Error: Fake Leads cannot exceed Total Leads'
        ELSE NULL 
    END AS error_message
FROM {{ ref('stg_leads_funnel') }}
WHERE 
    total_clicks > total_impressions 
    OR total_fake_leads > total_leads
