

with orders as (
    select * from snowflake_sample_data.tpch_sf1.orders
),

lineitems as (
    select * from snowflake_sample_data.tpch_sf1.lineitem
),

yearly_revenue as (
    select 
        extract(year from o.o_orderdate) as order_year,
        count(distinct o.o_orderkey) as total_orders,
        count(distinct o.o_custkey) as total_customers,
        sum(o.o_totalprice) as total_order_value,
        sum(l.l_extendedprice * (1 - l.l_discount)) as total_revenue,
        avg(o.o_totalprice) as avg_order_value,
        sum(l.l_quantity) as total_quantity_sold
    from orders o
    left join lineitems l
        on o.o_orderkey = l.l_orderkey
    group by extract(year from o.o_orderdate)
)

select * from yearly_revenue
order by order_year