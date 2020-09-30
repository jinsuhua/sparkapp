select
  case when c.product in ('MV51','CR51') then 5 else null end as platform,
  c.product, d.tm_vhc_vehicle_id, d.vin, c.prog_vin as prog_vin
from
  (select code_value, tc_sys_code_type_id
  from smcv.mes_tc_sys_code_list_his
  where loc = 'nj' and pt = '%s'
  ) a
inner join
  (select tc_sys_code_type_id
  from smcv.mes_tc_sys_code_type_his
  where loc='nj' and pt = '%s'
  and code_type='Key_OrderType'
  ) b
on a.tc_sys_code_type_id = b.tc_sys_code_type_id
inner join
  (select product, order_type, vin ,case when order_no like 'ST%%' then 1 else 0 end as prog_vin
  from %s
  where model_plant='1500'
  ) c
on a.code_value = c.order_type
inner join
  (select tm_vhc_vehicle_id, vin
  from %s
  ) d
on c.vin = d.vin
group by case when c.product in ('MV51','CR51') then 5 else null end, c.product, d.tm_vhc_vehicle_id, d.vin, c.prog_vin