CREATE OR REPLACE TABLE final as select * from (
with cohorts as (
    SELECT
        rp.join_dt
        , sm.size
        , l."Print Gender" as print_gender
        , count(distinct case when sd.customer_type = 'Returning' then sd.customer_id end) as returning_cohort
        , count(distinct case when sd.customer_type = 'First-time' then sd.customer_id end) as first_time_cohort 
        , sum(case when sd.customer_type = 'Returning' then sd.net_quantity end) as returning_sales
        , sum(case when sd.customer_type = 'First-time' then sd.net_quantity end) as first_time_sales

    FROM rolling_periods rp
    INNER JOIN sales_data sd on sd.day between rp.start_dt and rp.end_dt
    INNER JOIN size_map_hoja1 sm on sd.variant_title = sm.variant_title
    inner JOIN (select *, row_number() over(partition by Product order by Print) as rn from master_legend_legend) l on sd.product_title = l.Product and l.rn =1

    group by rp.join_dt
        , sm.size
        , l."Print Gender"
), tot_cohorts as (
    SELECT
        rp.join_dt
        , count(distinct case when sd.customer_type = 'Returning' then sd.customer_id end) as returning_cohort
        , count(distinct case when sd.customer_type = 'First-time' then sd.customer_id end) as first_time_cohort 
        , sum(case when sd.customer_type = 'Returning' then sd.net_quantity end) as returning_sales
        , sum(case when sd.customer_type = 'First-time' then sd.net_quantity end) as first_time_sales

    FROM rolling_periods rp
    INNER JOIN sales_data sd on sd.day between rp.start_dt and rp.end_dt

    group by rp.join_dt
        
        ),

base_data as (
SELECT
    a.product_type
    
    , a.size as variant
    , a.sku as sku
    , max(max(a.product_price)) over(partition by a.actual_drop_date, a.print, a.sku) as product_price
    , a.actual_drop_date
    , datepart('month', a.actual_drop_date) as drop_month
    , a.print
    , a.season as season
    , a.seasonality_determined as drop_time_holiday
    , a.print_gender
    , a.product_gender
    , a.main_color
    , a.design_category as designcat
    , a.design_elements_and_appeal as designelement
  
    , b.returning_cohort
    , b.first_time_cohort
    , b.returning_sales
    , b.first_time_sales
    , e.returning_cohort as rc_total
    , e.first_time_cohort as ft_total
    , e.returning_sales as rc_sales_total
    , e.first_time_sales as ft_sales_total
    , a.order_quantity
    , d."Confidence Score" as confidence_score
    , sum(case when a.days_from_drop <= 14 then a.net_quantity else 0 end) as sold_amount_14
    , sum(case when a.days_from_drop <= 30 then a.net_quantity else 0 end) as sold_amount_30
    , sum(case when a.days_from_drop <= 45 then a.net_quantity else 0 end) as sold_amount_45
    , sum(case when a.days_from_drop <= 60 then a.net_quantity else 0 end) as sold_amount_60
    , sum(case when a.days_from_drop <= 75 then a.net_quantity else 0 end) as sold_amount_75
    , sum(case when a.days_from_drop <= 90 then a.net_quantity else 0 end) as sold_amount_90
    

FROM joins_output a

inner join cohorts b 
    on DATE_TRUNC('month', a.actual_drop_date) = b.join_dt 
    and a.print_gender = b.print_gender 
    and a.size = b.size


left join confidence_scores d
    on a.actual_drop_date = d."Drop Date" and a.print = d.Print

inner join tot_cohorts e 
    on DATE_TRUNC('month', a.actual_drop_date) = e.join_dt 
    
where 
    a.customer_type is not null and a.days_from_drop <=90
    and a.product_type in ('Footie','Pajama Set','Romper','Convertible Romper','Baby Dress', 'Mom Pants', 'Jogger Set', 'Toddler Dress')

group by
    a.product_type 
    
    , a.size
    , a.sku
    
    , a.actual_drop_date
    , datepart('month', a.actual_drop_date)
    , a.print
    , a.season 
    , a.seasonality_determined
    , a.print_gender
    , a.product_gender
    , a.main_color
    , a.design_category
    , a.design_elements_and_appeal
    
    , b.returning_cohort
    , b.first_time_cohort
    , b.returning_sales
    , b.first_time_sales
    , e.returning_cohort
    , e.first_time_cohort
    , e.returning_sales
    , e.first_time_sales
    , a.order_quantity
    , d."Confidence Score")
, drops as (
    select distinct print, actual_drop_date from base_data
),
products as (
    select  
        print
        , actual_drop_date
        , variant
        , max(case when product_type = 'Convertible Romper' then 1 else 0 end) as convertible_romper_in_drop
        , max(case when product_type = 'Pajama Set' then 1 else 0 end) as pajama_set_in_drop
        , max(case when product_type = 'Romper' then 1 else 0 end) as romper_in_drop
        , max(case when product_type = 'Footie' then 1 else 0 end) as footie_in_drop
    from base_data
    group by 
        print
        , actual_drop_date
        , variant 
    ),
md as (
    select 
    d.actual_drop_date
    , ms.product_print_title as print
    , sum(ms.spend) as marketing_spend
    from marketing_spend_agg ms
    inner join drops d on ms.product_print_title = d.print and ms.date between d.actual_drop_date and d.actual_drop_date+30

    group by 
    d.actual_drop_date
    , ms.product_print_title
)
    select 
    bd.*
    , case when bd.sold_amount_14 >= bd.order_quantity*.95 then 1 else 0 end as sellout_flg
    , md.marketing_spend/90 as marketing_spend_drop
    , case when md.marketing_spend >0 then 1 else 0 end as marketing_flag
    , p.convertible_romper_in_drop
    , p.pajama_set_in_drop
    , p.romper_in_drop
    , p.footie_in_drop
    , pd.prints_in_drop
    , case when bd.sold_amount_14 >= bd.order_quantity*.95 then bd.order_quantity*2 else bd.order_quantity end as corrected_order_amt
    , sum(bd.sold_amount_90) over(partition by bd.print, bd.actual_drop_date) as total_sales_drop
    , bd.sold_amount_90 / sum(bd.sold_amount_90) over(partition by bd.print, bd.actual_drop_date) as proportion
    , bd.order_quantity / sum(bd.order_quantity) over(partition by bd.print, bd.actual_drop_date) as order_proportion

    from base_data bd
    left join md on bd.actual_drop_date = md.actual_drop_date and bd.print = md.print
    left join products p on bd.print = p.print and bd.actual_drop_date = p.actual_drop_date and bd.variant = p.variant
    left join (select distinct actual_drop_date, count(distinct print) as prints_in_drop from base_data group by actual_drop_date) pd
    on bd.actual_drop_date = pd.actual_drop_date)