
    
    

select
    o_orderkey as unique_field,
    count(*) as n_records

from DEMO_DB.TPCH_SF1_STAGING_STAGING.stg_orders
where o_orderkey is not null
group by o_orderkey
having count(*) > 1


