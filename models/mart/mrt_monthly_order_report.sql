WITH monthly_users_recap AS (
    SELECT DATE_TRUNC(order_date, month) as order_month,
    COUNT(DISTINCT user_name) as total_monthly_users
    FROM {{source('sales_database', 'order')}}
    GROUP BY 1
    ORDER BY total_monthly_users DESC

),

total_monthly_user_from_jawa_timur as (
    SELECT DATE_TRUNC(order_date, month) as order_month,
    COUNT(DISTINCT user.user_name) as total_monthly_users_from_jawa_timur
    FROM {{source('sales_database', 'order')}} as orders
    LEFT JOIN {{source('sales_database', 'user')}} as user 
    ON 
    user.user_name = orders.user_name
    WHERE user.customer_state LIKE '%JAWA%TIMU%'
    GROUP BY order_month
),

monthly_order_recap as (
    SELECT DATE_TRUNC(order_date, month) as order_month,
    COUNT(order_id) as total_monthly_orders
    FROM {{source('sales_database', 'order')}}
    GROUP BY order_month
),

shipping_cost as (
    SELECT shipping_cost
    FROM sales_database.order_item
    WHERE price > 7000
)


SELECT u.order_month,
COALESCE(u.total_monthly_users, 0) as nb_users_users_monthly,
COALESCE(jt.total_monthly_users_from_jawa_timur, 0) as total_monthly_user,
COALESCE(o.total_monthly_orders, 0) as monthly_order_count
FROM monthly_users_recap as u 
LEFT JOIN total_monthly_user_from_jawa_timur as jt
ON 
jt.order_month = u.order_month
LEFT JOIN monthly_orders_recap as o
ON 
o.order_month = u.order_month
ORDER BY order_month