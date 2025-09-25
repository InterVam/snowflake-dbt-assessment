{{
  config(
    materialized='table'
  )
}}

with orders as (
    select * from {{ source('tpch', 'orders') }}
),

lineitems as (
    select * from {{ source('tpch', 'lineitem') }}
),

customers as (
    select * from {{ source('tpch', 'customer') }}
),

order_revenue as (
    select 
        l.l_orderkey,
        sum(l.l_extendedprice * (1 - l.l_discount)) as order_revenue
    from lineitems l
    group by l.l_orderkey
),

customer_revenue_calc as (
    select 
        o.o_custkey,
        c.c_name as customer_name,
        c.c_mktsegment as market_segment,
        count(distinct o.o_orderkey) as total_orders,
        sum(o.o_totalprice) as total_order_value,
        sum(coalesce(or.order_revenue, 0)) as total_revenue,
        min(o.o_orderdate) as first_order_date,
        max(o.o_orderdate) as last_order_date,
        avg(o.o_totalprice) as avg_order_value
    from orders o
    left join customers c
        on o.o_custkey = c.c_custkey
    left join order_revenue or
        on o.o_orderkey = or.l_orderkey
    group by o.o_custkey, c.c_name, c.c_mktsegment
)

select * from customer_revenue_calc
order by total_revenue desc