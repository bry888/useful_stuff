# kupowali w kategorii

SELECT count(distinct tra.buyer_id) FROM

(SELECT category_id, _PARTITIONDATE as data
FROM `schema.base.category_tree`, UNNEST(name_path) as name_path_unnest
WHERE _PARTITIONDATE between '2019-03-01' and '2019-03-01'
AND name_path_unnest IN ('Zabawki')) cat

JOIN

(SELECT buyer_id, item_category_id, _PARTITIONDATE
from `schema.base.transactions`
where
_PARTITIONDATE between '2019-03-01' and '2019-03-01') tra

ON cat.data = tra._PARTITIONDATE and cat.category_id = tra.item_category_id 
;

# unnest zamienia listę tupli (czyli kolejnych wartosci w kolumnach) na wartości w tylu kolumnach ile jest ich w tuplu:
INSERT dataset.Warehouse (warehouse, state)
SELECT *
FROM UNNEST([('warehouse #1', 'WA'),
      ('warehouse #2', 'CA'),
      ('warehouse #3', 'WA')])

|  warehouse   | state |
+--------------+-------+
| warehouse #1 | WA    |
| warehouse #2 | CA    |
| warehouse #3 | WA    |



# json extract

select _PARTITIONDATE, event.action
from `schema.base.events`
where
_PARTITIONDATE between '2018-12-01' and '2018-12-01' 
and event.category = 'video'
and JSON_EXTRACT(custom_params.values_raw, '$._opbox.boxName') = 'trailer-smoka'
limit 6
;





# sprzedaz sklepu po dniach

select sprzedaz.seller_id, daty.day as `data`, sprzedaz.transactions as tranzakcje, coalesce(sprzedaz.quantity,0) as przedmioty, coalesce(sprzedaz.gmv,0) as gmv from (

(SELECT day
 FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2015-09-01'), DATE('2015-09-01'), INTERVAL 1 DAY)
 ) AS day
) daty

left join (select seller_id, _PARTITIONDATE, sum(cast(transaction_cost as numeric)) gmv, sum(item_quantity) quantity, count(*) transactions 
from `schema.base.transactions`
where
((seller_id = '123' and lower(item_name) like '%finish%') or seller_id = '234')
and
_PARTITIONDATE between '2015-09-01' and '2015-09-01'
group by seller_id, _PARTITIONDATE) sprzedaz

on sprzedaz._PARTITIONDATE = daty.day)

order by daty.day desc;



# parsowanie ISO date #

select status.triggered,
timestamp(substr(status.triggered,1,26)) utc_time,
STRING(timestamp(substr(status.triggered,1,26)), "Europe/Warsaw") local_time
from `schema.base.payments` 
where _PARTITIONDATE = '2019-04-01' LIMIT 10



# filtrowanie dziwnych struktur

select account_flags, flag from `schema.base.lw_tmp`
cross join unnest(account_flags) as flag
where ARRAY_LENGTH(account_flags) > 0 and flag.value[ordinal(1)] and flag.key[ordinal(1)] = 'verified'

SELECT 
  u.account_id
  , u.login
  , account_create_date
FROM `schema.base.users` u, UNNEST(account_flags) as account_flag
WHERE u._PARTITIONDATE = '2019-05-08'
AND account_status = 'active'
AND ('verifiedcompany' = (Select key from unnest(account_flag.key) key)
AND TRUE = (Select value from unnest(account_flag.value) value))
AND account_create_date BETWEEN '2017-01-01' AND '2018-10-31'
GROUP BY   u.account_id
  , u.login
  , account_create_date





/* show partitionsa - podobno za darmo */
select _PARTITIONDATE as v_date,
count(1)
from
`schema.base.order` where _partitiondate >= '2000-01-01'
GROUP by _PARTITIONDATE
order by 1


# czad! select * from `schema.base.events_*` where _TABLE_SUFFIX IN ("MOBILE_EVENT", "PAGE_VIEW")
SELECT
      substr(cast(aa._PARTITIONTIME as string),1,10) as v_date
     ,substr(cast(aa._PARTITIONTIME as string),1,7) as v_date_month
     ,count(distinct page_view_id)
FROM `schema.base.events_*` aa
WHERE
     aa._PARTITIONTIME between  '2019-02-01' and '2019-02-01'
     AND _TABLE_SUFFIX IN (
     --"MOBILE_EVENT",
     --"PAGE_VIEW",
     "EVENT")
     and aa.event.category = 'selection.form.v2'
     and JSON_EXTRACT_SCALAR(custom_params.values_raw, '$._opbox.boxName') = 'search.cars'
     and event.label = 'submitForm'
group by
        v_date
        ,v_date_month




CREATE TABLE `schema.base.bg_reckitt_utm_data` 
(utm_source string,
utm_medium string,
utm_campaign string,
utm_content string,
visits int64,
visit_page_views int64,
bounced int64,
sum_seconds_duration int64,
users int64,
page_views int64,
bounce_rate float64,
avg_time_on_site_in_seconds float64,
year_month date)
PARTITION BY year_month

# RODO
ALTER TABLE `schema.base.bg_reckitt_utm_data`
 SET OPTIONS (
   labels=[("pers__id", "0"), ("trunc_dataset", "1")]
 )





create table `schema.base.test` (costam string, ble int64, day date) partition by day


insert `schema.base.test` (costam, ble, day) values ('heheddddj', 10, '2018-09-09'), ('dfdgdg', 10, '2018-09-09')

select * from `schema.base.test`


https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_statement

update `schema.base.test`
set ble = 20
where day = '2018-09-09';
# podmienilo 10 na 20 dla takich dat

INSERT `schema.base.test` (costam, ble, day)
SELECT costam, ble, day
FROM `schema.base.test`
# skladnia inserta z selecta



DELETE FROM `schema.base.test` WHERE true ### DELETE ALL ROWS! 'Where' is mandatory

~update partition:
DELETE FROM `schema.base.test` WHERE day = '2018-09-10';
INSERT `schema.base.test` select * from OBLICZENIE

# BQ update using joins
https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_using_joins




# rankowanie standardowo
SELECT firstname, department, startdate,
  RANK() OVER ( PARTITION BY department ORDER BY startdate ) AS rank
FROM Employees;




select 
custom_params.values_extracted

from
`schema.base.events`
cross join unnest(custom_params.values_extracted) cp
where _PARTITIONDATE='2019-07-21'
and  cp.key[offset(0)]='_opbox'
and jSON_EXTRACT_scalar(cp.value[offset(0)],'$.boxName') = 'delivery.expenses'
limit 1000





SELECT count(distinct ord.buyer_id) FROM

(SELECT category_id, _PARTITIONDATE as data
FROM `schema.base.category_tree`, UNNEST(id_path) as id_path_unnest
WHERE _PARTITIONDATE between DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND id_path_unnest IN ('123', '345')) cat

JOIN

(SELECT buyer_id, cast(category_id as string) as ord_cat, _PARTITIONDATE
from `schema.base.order`, UNNEST(features) as feature
where
feature = 'invoiceRequired'
and
_PARTITIONDATE between DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ord

ON cat.data = ord._PARTITIONDATE and cat.category_id = ord.ord_cat
