-- Tags: stateful, no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/74716

select StartDate, TraficSourceID in (0) ? 'type_in' : 'other' as traf_type, sum(Sign)
from test.visits 
where CounterID = 842440
group by StartDate, traf_type ORDER BY StartDate, traf_type
