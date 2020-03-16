create table table_to_create nologging as
select	distinct
	a.cola,
	b.colb,
	decode(a.decode_condition, 1, '是', '否') as decoded,
	row_number() over(partition by a.class_condition order by a.rand_condition desc) as rn
from	fscrm.table_a a
left join hello.hello e
left join (select * from table_c c where c.something='something' and c.num=1234) b on a.clo1 = b.col1
where	a.compare_condition=b.compare_condition
and 	a.num not in (1, 2, 3)
and 	not exists (select d.cold from table_d where a.cola=d.cold)
order by a.order_condition;
