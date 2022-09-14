{% set table_status = engine.execute(text("select table_name from information_schema.tables where table_name = 'stage_transform'")) %}

{% if table_status.fetchone()[0] == table_name %}
    drop table {{ table_name }};
{% endif %}

create table {{ table_name }} as 
    select 
        trade_id,
        timestamp,
        exchange,
        price,
        size,
        substring(trade_conditions, 4,1) as trade_condition
    from public.raw_trades; 
