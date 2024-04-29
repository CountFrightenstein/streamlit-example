CREATE OR REPLACE TABLE legend_map as 
    SELECT 
        * 
        , row_number() over(partition by variant_sku order by ns desc) as rnd
    FROM master_legend_legend ml
    inner join (select product_title, variant_sku, sum(net_quantity) as ns from sales_data group by 1,2) sd on ml.Product = sd.product_title
    