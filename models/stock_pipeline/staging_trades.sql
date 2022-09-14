drop table if exists {{ target_table }};

create table kingmoh.public.{{ target_table }} as 
    select 
        trade_id,
        timestamp,
        exchange,
        price,
        size,
        substring(trade_conditions, 4,1) as trade_condition
    from public.raw_trades; 
