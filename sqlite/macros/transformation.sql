
{% macro normalize_text(col) %}
  lower(
    trim(
      replace(
        replace(
          replace(
            replace({{ col }}, ' -', '-'),  -- Remove spaces before hyphen
            '- ', '-'  -- Remove spaces after hyphen
          ),
          '  ', ' '  -- Replace double spaces with a single space
        ),
        'demand gen', 'demandgen'  -- Normalize known variations
      )
    )
  )
{% endmacro %}

{% macro intify(col) %}
  -- Convert column to integer:
  case
    -- Case 1: If NULL or empty string, return 0
    when {{ col }} is null or trim({{ col }}) = '' 
      then 0
    -- Case 2: If purely numeric, cast to int
    when {{ col }} GLOB '[0-9]*'
         and length({{ col }}) > 0
      then cast({{ col }} as int)
    -- Case 3: If anything else (non-numeric), return 0
    else 0
  end
{% endmacro %}

{% macro clean_id(col) %}
  -- Convert campaign_id into a clean string format
  case
    -- Case 1: If it's a valid scientific notation (E+), convert to integer first then to text
    when {{ col }} like '%E+%' 
      then cast(cast({{ col }} as int) as text)
    -- Case 2: If it's a whole number with .000 at the end, remove decimals and convert to string
    when {{ col }} GLOB '[0-9]+.000*'  
      then cast(cast({{ col }} as int) as text)  
    -- Case 3: If it's a valid number with decimals, store it as float then text
    when {{ col }} like '%.%' 
      then cast(cast({{ col }} as decimal) as text)  
    -- Case 4: If it's already a string, keep it as is
    else cast({{ col }} as text)  
  end
{% endmacro %}



