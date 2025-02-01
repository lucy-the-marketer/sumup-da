SELECT 
    campaign_id, 
    date, 
    country_code,
    nb_of_sessions, 
    nb_of_orders, 
    nb_of_poslite_items_ordered,
    nb_of_valid_order,
    CASE 
        WHEN nb_of_orders > nb_of_sessions 
            THEN 'Error: Orders cannot exceed Sessions'
        WHEN nb_of_valid_order = 1 AND (nb_of_orders < 1 OR nb_of_poslite_items_ordered < 1)
            THEN 'Error: Valid Orders must have at least 1 Order and 1 Item Ordered'
        ELSE NULL 
    END AS error_message
FROM {{ ref('stg_web_orders') }}
WHERE 
    nb_of_orders > nb_of_sessions 
    OR (nb_of_valid_order = 1 AND (nb_of_orders < 1 OR nb_of_poslite_items_ordered < 1))
