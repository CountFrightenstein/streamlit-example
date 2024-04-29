create or replace table prod_var_prop as 
select
    product_type
    , variant
    , avg(order_proportion) as order_proportion

from final

group by   
    product_type
    , variant