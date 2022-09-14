{# Transformation after loading 

Parameters
----------
target_table: str 
	The target table 

#}


{% set table_status = engine.execute(text("select table_name from information_schema.tables where table_name = '{{ target_table }}'")) %}


{% if table_status.fetchone()[0] %}
    drop table {{ target_table }};
{% endif %}

create table {{ target_table }} as
    select
        trade_id,
        timestamp,
        exchange,
        price,
        size,
        case
        {% for code, name in exchange_code.items() %}
            when exchange = {{ code }} then {{ name }}
        {% endfor %}
            else "UNK"
        end as exchange_name,
            substring(trade_conditions, 4,1) as trade_condition
    from
	    public.raw_trades;
