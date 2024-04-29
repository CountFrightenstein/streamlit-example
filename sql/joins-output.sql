CREATE OR REPLACE TABLE joins_output as

    SELECT 
        l.product_title
        , l.Type as product_type
        /*,  REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(sd.variant_title, '\\s', '', 'g'), -- Removes all spaces
          '^[^0-9]*([/|])', '\\1', 'g'), -- Removes non-numeric characters before / or |
        '^.*TOG', '', 'g'), -- Replaces anything up to and including "TOG"
      '[^A-Za-z0-9]', '', 'g' -- Removes all non-alphanumeric characters
    ) as variant_title*/
        , sm.size
        , sd.day
        , sd.variant_sku
        , sd.customer_type
        , sd.order_id
        , sd.order_name
        , sd.customer_id
        , sd.customer_name
        , sd.product_price
        , sd.net_quantity
        , sd.gross_sales
        , sd.discounts
        , sd.returns
        , sd.net_sales
        , sd.total_sales
        , sd.total_cost
        , sd.variant_sku as SKU
        , sds.Drop_Dates as bad_drop_date
        , ifnull(sds.Season, 'Winter') as Season
        , l.Product as product
        , l.Print as print
        , l."Seasonality Determined" as seasonality_determined
        , l."Print Gender" as print_gender
        , l."Product Gender" as product_gender	
        , l."Main Color" as main_color	
        , l."Design Category" as design_category	
        , l."Design Elements and Appeal" as design_elements_and_appeal
        , ol."Drop Date" as orig_drop_date
        , ol.Quantity as ordered_quantity
        , ol."Order Name" as orig_order_name
        , ol.next_drop
        , olf.actual_drop_date
        , olf.rn
        , olf.order_quantity
        , sd.day-ol."Drop Date" as days_from_drop
        , ms.spend as marketing_spend
    FROM sales_data sd
    LEFT JOIN sales_data_sku sds on sd.variant_sku = sds.SKU 
    INNER JOIN size_map_hoja1 sm on sd.variant_title = sm.variant_title
    INNER JOIN legend_map l on sd.variant_sku = l.variant_sku and l.rnd =1
    INNER JOIN (
        SELECT 
            SKU
            , "Drop Date"
            , Quantity
            , "Order Name"
            
            , lead("Drop Date") over(partition by SKU order by "Drop Date") as next_drop
        from order_log_data) 

    ol on sd.variant_sku = ol.SKU and sd.day between ol."Drop Date" 
    and 
    case when
    ifnull(ol.next_drop-1, ol."Drop Date"+90) >= ol."Drop Date"+90 
    then ol."Drop Date"+90 else ifnull(ol.next_drop-1, ol."Drop Date"+90) 
    end
    
    INNER JOIN (select 
        SKU
        , "Drop Date" as actual_drop_date
        , row_number() over(partition by SKU order by "Drop Date") as rn 
        , Quantity as order_quantity
    from order_log_data 
    where lower("Order Name") not like '%reorder%') olf on olf.SKU = sd.variant_sku and olf.rn=1

    left join marketing_spend_agg ms on sd.day = ms.date and ms.Product_Print_Title = l.Print
    where year(ol."Drop Date") in (2022, 2023, 2024) and sd.product_type is not null