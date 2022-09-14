{% set table_status = engine.execute(text("select table_name from information_schema.tables where table_name = 'stage_transform'")) %}

{% if table_status.fetchone()[0] == target_table %}
    drop table {{ target_table }};
{% endif %}

create table kingmoh.public.{{ target_table }} as 
    select 
        trade_id,
        timestamp,
        exchange,
        price,
        size,
        substring(trade_conditions, 4,1) as trade_condition
    from kingmoh.public.raw_trades; 
