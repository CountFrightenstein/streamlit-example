CREATE OR REPLACE TABLE rolling_periods (start_dt DATE, end_dt DATE, join_dt DATE);

-- Insert the rolling 3-month periods with the end date adjusted to the last day of the month
WITH RECURSIVE dates AS (
  SELECT DATE '2022-01-01' AS dt -- Starting from January 2022
  UNION ALL
  SELECT dt + INTERVAL 1 MONTH FROM dates WHERE dt < DATE '2024-02-01' -- Up to October 2023 for the start dates
)
INSERT INTO rolling_periods (start_dt, end_dt, join_dt)
SELECT 
  dt, 
  DATE_TRUNC('month', dt + INTERVAL 3 MONTH) - INTERVAL '1 day' AS end_dt, -- Adjust the end date to the last day of the 3rd month
  DATE_TRUNC('month', dt + INTERVAL 3 MONTH) AS join_dt
FROM dates;