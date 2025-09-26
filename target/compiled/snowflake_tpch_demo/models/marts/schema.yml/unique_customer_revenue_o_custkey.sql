
    
    

select
    o_custkey as unique_field,
    count(*) as n_records

from DEMO_DB.TPCH_SF1_STAGING_marts.customer_revenue
where o_custkey is not null
group by o_custkey
having count(*) > 1


