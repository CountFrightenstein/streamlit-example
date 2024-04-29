with all_variants as (select distinct product_type, variant from final),
cohorts as (
    select 
        product_type
        , variant
        , print_gender
        , max(returning_cohort) as returning_cohort
        , max(first_time_cohort) as first_time_cohort
        , max(returning_sales) as returning_sales
        , max(first_time_sales) as first_time_sales
    from final

    where actual_drop_date between '2023-12-01' and '2024-01-31'

    group by 
        product_type
        , variant
        , print_gender
        ),
base_data as (
select
    av.product_type
    , av.variant
    , cast(nd.actual_drop_date as date) as actual_drop_date
    , datepart('month', cast(nd.actual_drop_date as date)) as drop_month
    , nd.print
    , nd.season
    , nd.drop_time_holiday
    , nd.print_gender
    , nd.main_color
    , nd.designcat
    , nd.designelement
    , c.returning_cohort
    , c.first_time_cohort
    , c.returning_sales
    , c.first_time_sales
    , nd.order_quantity
    , nd.confidence_score
    , nd.convertible_romper_in_drop
    , nd.pajama_set_in_drop
    , nd.romper_in_drop
    , nd.footie_in_drop
    , nd.prints_in_drop
    , pvp.order_proportion
    , sm.size_sort
from new_drop nd
cross join all_variants av
inner join prod_var_prop pvp
    on av.product_type = pvp.product_type and av.variant = pvp.variant
inner join cohorts c   
    on av.product_type = c.product_type and av.variant = c.variant and nd.print_gender = c.print_gender
inner join (select size as size, min("Size Sort") as size_sort from size_map_hoja1 group by size) sm
    on av.variant = sm.size
where 
(nd.pajama_set_in_drop = 'Yes' and av.product_type = 'Pajama Set') or
(nd.convertible_romper_in_drop = 'Yes' and av.product_type = 'Convertible Romper') or
(nd.romper_in_drop = 'Yes' and av.product_type = 'Romper') or
(nd.footie_in_drop = 'Yes' and av.product_type = 'Footie') or
(av.product_type not in ('Pajama Set', 'Convertible Romper', 'Romper', 'Footie'))
)

select 
    bd.product_type
    , bd.variant
    , bd.actual_drop_date
    , bd.drop_month
    , bd.print
    , bd.season
    , bd.drop_time_holiday
    , bd.print_gender
    , bd.main_color
    , bd.designcat
    , bd.designelement
    , bd.returning_cohort
    , bd.first_time_cohort
    , bd.returning_sales
    , bd.first_time_sales
    , (bd.order_proportion/sum(order_proportion) over(partition by print)) * bd.order_quantity as order_quantity
    , bd.confidence_score
    , CASE WHEN bd.convertible_romper_in_drop = 'Yes' then 1 else 0 end as convertible_romper_in_drop
    , CASE WHEN bd.pajama_set_in_drop = 'Yes' then 1 else 0 end as pajama_set_in_drop
    , CASE WHEN bd.romper_in_drop = 'Yes' then 1 else 0 end as romper_in_drop
    , CASE WHEN bd.footie_in_drop = 'Yes' then 1 else 0 end as footie_in_drop
    , bd.prints_in_drop
    , bd.order_quantity as total_buy
    , bd.size_sort
from base_data bd