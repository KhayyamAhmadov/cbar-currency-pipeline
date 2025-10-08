-- 1. Valyuta üzrə minimum və maksimum məzənnə (ümumi tarix boyu)
select code as currency, min(rate) as min_rate, max(rate) as max_rate from public.exchange_rates
group by code


-- 2. Son tarix üzrə bütün valyutaların məzənnəsi
select date, name, code, rate from public.exchange_rates
where date = (select max(date) from public.exchange_rates)
order by code


-- 3. Müəyyən valyuta üçün tarix aralığında məzənnələr
select date, rate from public.exchange_rates
where code = 'USD' and date between '2020-01-01' and '2025-10-07'


-- 4. Hər valyutanın əvvəlki günlə müqayisədə məzənnə dəyişimi
select e1.date, e1.code, e1.rate as current_rate, e2.rate as previous_rate, e1.rate - e2.rate as rate_change from public.exchange_rates as e1
left join public.exchange_rates as e2 on e1.code = e2.code and e1.date = e2.date + interval '1 day'
order by e1.code, e1.date


--5. Hər valyuta üçün orta məzənnə
select  code, avg(rate) as avg_rate from exchange_rates
group by code
order by code;


-- 6. Son 7 günün trendlərini göstərən query
select * from exchange_rates
where date >= (select max(date) - INTERVAL '6 days' from exchange_rates)
order by code, date;


-- 7️. Məzənnəsi müəyyən səviyyədən yüksək olan valyutalar
select * from exchange_rates
where rate > 100
order by rate desc


-- 8. Son ayın orta məzənnəsi və dəyişiklik faizi
with last_month as(
	select code, name, avg(rate) as avg_rate, min(rate) as min_rate, max(rate) as max_rate from public.exchange_rates
	where date >= CURRENT_DATE - interval '30 days'
	group by code, name
)
select code, name, 
	round(avg_rate, 4) as avg_rate_30d,
	round(min_rate, 4) as min_rate_30d,
	round(max_rate, 4) as max_rate_30d,
	round(((max_rate - min_rate) / nullif(min_rate, 0) * 100), 2) as volatility_percent
from last_month
order by volatility_percent desc


-- 9. Valyuta korrelyasiyası (USD ilə digər valyutaların əlaqəsi)
with usd_rates as (
    select date, rate as usd_rate from exchange_rates
    where code = 'USD')
select e.code, e.name, corr(e.rate, u.usd_rate) as correlation_with_usd, count(*) as data_points from exchange_rates e
inner join usd_rates u on e.date = u.date
where e.code != 'USD'
group by e.code, e.name
having count(*) > 100
order by correlation_with_usd desc


-- 10. Həftənin günlərinə görə orta məzənnə (hansı gün daha yüksək/aşağı olur)
select code, name, 
	TO_CHAR(date, 'Day') as day_of_week, 
	extract(DOW from date) as day_number, 
	round(avg(rate), 4) AS avg_rate, 
	count(*) as records 
	from exchange_rates
where code in ('USD', 'EUR', 'GBP', 'RUB')
group by code, name, TO_CHAR(date, 'Day'), extract(DOW from date)
order by code, day_number;


-- 11. İllik məzənnə statistikası
select code, name, extract(year from date) as year,
    round(avg(rate), 4) as avg_rate,
    round(min(rate), 4) as min_rate,
    round(max(rate), 4) as max_rate,
    ROUND(STDDEV(rate), 4) AS std_deviation,
    count(*) as days_recorded
from exchange_rates
group by code, name, extract(year from date)
order by code, year desc


-- 12. Hər ayın ilk və son günü məzənnə müqayisəsi
with monthly_rates as (
    select code, name, DATE_TRUNC('month', date) as month,
        first_value(rate) over (partition by code, DATE_TRUNC('month', date) order by date) AS first_rate,
        last_value(rate) over (partition by code, DATE_TRUNC('month', date) order by date rows between unbounded preceding and unbounded following) as last_rate
    from exchange_rates)
select distinct code, name, month,
    round(first_rate, 4) as month_start_rate,
    round(last_rate, 4) as month_end_rate,
    round(last_rate - first_rate, 4) AS monthly_change,
    round(((last_rate - first_rate) / nullif(first_rate, 0) * 100), 2) AS monthly_change_percent
from monthly_rates
where month >= CURRENT_DATE - INTERVAL '1 year'
order by code, month desc


-- 13. Mövsümi trend analizi (hansı ayda daha yüksək/aşağı)
select code, name,
    extract(month from date) as month_number,
    TO_CHAR(DATE_TRUNC('month', date), 'Month') AS month_name,
    round(avg(rate), 4) as avg_rate,
    round(min(rate), 4) as min_rate,
    round(max(rate), 4) as max_rate,
    count(*) as records
from exchange_rates
where code in ('USD', 'EUR', 'GBP')
group by code, name, extract(month from date), TO_CHAR(DATE_TRUNC('month', date), 'Month')
order by code, month_number;


-- 14. Moving Average (7, 30, 90 günlük hərəkətli orta)
SELECT code, name, date, rate,
    round(avg(rate) over (partition by code order by date rows between 6 PRECEDING AND CURRENT ROW), 4) AS ma_7d,
    round(avg(rate) over (partition by code order by date rows between 29 PRECEDING AND CURRENT ROW), 4) AS ma_30d,
    round(avg(rate) over (partition by code order by date rows BETWEEN 89 PRECEDING AND CURRENT ROW), 4) AS ma_90d
FROM exchange_rates
WHERE code = 'USD' AND date >= CURRENT_DATE - INTERVAL '6 months'
ORDER BY date DESC;


-- 15. Valyuta çəkisi/dominantlığı (hansı valyuta bazarda ən populyardır)
select code, name,
    count(*) AS total_records,
    round(count(*) * 100.0 / (select count(*) from exchange_rates), 2) as market_share_percent,
    min(date) as first_appearance,
    max(date) as last_update,
    max(date) - min(date) AS data_span_days
from exchange_rates
group by code, name
order by total_records desc