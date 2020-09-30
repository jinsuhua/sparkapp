with t as
(
  select n.platform, n.product, n.vin, m.tm_bas_uloc_id
  from %s m
  inner join (select * from %s where prog_vin=0) n
  on m.tm_vhc_vehicle_id = n.tm_vhc_vehicle_id
  where scan_time >= '%s' and scan_time < '%s'
  and m.tm_bas_uloc_id in
  (104040,
  104167,
  104323,
  104179,
  104251,
  104322,
  104223,
  104383,
  103844,
  104237)
  group by n.platform, n.product, n.vin, m.tm_bas_uloc_id
)
select '%s' as data_date, platform, product, type, count(vin) as num from
(
select
m.platform,
m.product,
'making_csh' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104040 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104167) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'corridor_csh' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104167 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104323) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'white_wbs' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104323 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104179) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'making_tzh' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104179 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104251) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'corridor_tzh' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104251 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104322) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'paint_bdc' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104322 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104223) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'sort_path' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104223 ) m
left join
(select product, vin from t where tm_bas_uloc_id in (104383, 103844) group by product,vin ) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'making_zzh' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=104383 ) m
left join
(select product, vin from t where tm_bas_uloc_id=103844) n
on m.product = n.product and m.vin=n.vin
where n.vin is null

union all

select
m.platform,
m.product,
'hold_vehicle' as type,
m.vin
from
(select platform, product, vin from t where tm_bas_uloc_id=103844 ) m
left join
(select product, vin from t where tm_bas_uloc_id=104237) n
on m.product = n.product and m.vin=n.vin
where n.vin is null
) qqq1
group by platform, product, type