{{
  config(
    materialized='table'
  )
}}

with orders as (
    select * from {{ source('tpch', 'orders') }}
),

customers as (
    select * from {{ source('tpch', 'customer') }}
),

orders_with_customers as (
    select 
        o.o_orderkey,
        o.o_custkey,
        o.o_orderstatus,
        o.o_totalprice,
        o.o_orderdate,
        o.o_orderpriority,
        o.o_clerk,
        o.o_shippriority,
        o.o_comment,
        c.c_name as customer_name,
        c.c_address as customer_address,
        c.c_phone as customer_phone,
        c.c_acctbal as customer_account_balance,
        c.c_mktsegment as customer_market_segment,
        extract(year from o.o_orderdate) as order_year,
        o.o_totalprice as total_price
    from orders o
    left join customers c
        on o.o_custkey = c.c_custkey
)

select * from orders_with_customers