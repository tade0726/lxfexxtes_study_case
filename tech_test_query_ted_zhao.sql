-- task 2:

/*
Constraints
- [x] work even there is no data on a particular day in this period within the data
- [x] only include users existing
- [x] enable accounts only
- [x] order by row_number in descending
- [x] fix QA issues
 */


-- issue 1: looks like a datetime typo for close time, trade over 2 years

with c1 as (
        select trades.open_time, trades.close_time, (trades.close_time - trades.open_time) as duration from trades
    )
select * from c1 where duration > '720 day';

-- issue 2: will login hash + server hash be the unique identifier?
select count(*) from (select distinct users.login_hash, users.server_hash from users where enable = 1) as t1
UNION ALL
select count(*) from users;


select login_hash, server_hash, count(*) from users group by login_hash, server_hash;

select * from users where login_hash = '18D4C2E739573770F9DF198F0E51C1B9' and server_hash = '3D1F7E00251C43107EF39F55300781DB';

select count(*) from (select distinct * from users) as t1
UNION ALL
select count(*) from (select distinct login_hash, server_hash from users) as t2;


--- issue 3: Deduplication on trades data:
select count(*) from (select distinct ticket_hash, login_hash, server_hash from trades) t1
UNION ALL
select count(*) from (select distinct * from trades) t2;

select login_hash, server_hash, count(*) from trades group by login_hash, server_hash ;



-- final solution

with cleaned_trades as (
    -- clean erratic close_time, where I presume add on 2 years by mistake
    select
        *,
        (close_time - open_time) as duration,
        case
            when (close_time - open_time) >= interval '730 days'
            then close_time - interval '730 days'
            else close_time
        end as adjusted_close_time,
        case
            when (close_time - open_time) >= interval '730 days'
            then (close_time - interval '730 days' - open_time)
            else (close_time - open_time)
        end as adjusted_duration
    from trades
), cleaned_users as (
    -- remove duplications, there are duplications
    select distinct * from users where enable = 1
), merged_trades as (

    select
        adjusted_close_time as dt_report,
        adjusted_duration as duration,
        cleaned_trades.open_time,
        cleaned_trades.login_hash,
        cleaned_trades.server_hash,
        cleaned_trades.symbol,
        cleaned_users.currency,
        cleaned_trades.volume
    from cleaned_trades join cleaned_users on cleaned_users.login_hash = cleaned_trades.login_hash and cleaned_users.server_hash = cleaned_trades.server_hash
    -- remove record that open and close time are exactly the same
    where adjusted_close_time != open_time
)
, f1 as (
-- sum_volume_prev_7d
-- sum_volume_prev_all
-- date_first_trade
-- row_number
select dt_report,
                login_hash,
                server_hash,
                symbol,
                currency,
                sum(volume)
                over (partition by login_hash, server_hash, symbol order by dt_report range between '7 days' PRECEDING and CURRENT ROW )  as sum_volume_prev_7d,
                sum(volume)
                over (partition by login_hash, server_hash, symbol order by dt_report range between unbounded PRECEDING and CURRENT ROW ) as sum_volume_prev_all,
                first_value(dt_report) over (partition by login_hash, server_hash, symbol order by dt_report) as date_first_trade,
                row_number() over (partition by 1 order by dt_report, login_hash, server_hash, symbol) as row_number
         from merged_trades)

, f2 as (
-- rank_volume_symbol_prev_7d
-- rank_count_prev_7d
    SELECT
        t.dt_report,
        t.login_hash,
        t.server_hash,
        t.symbol,
        t.currency,
        DENSE_RANK() OVER (
          PARTITION BY t.dt_report
          ORDER BY t.volume_7d DESC
        ) AS rank_volume_symbol_prev_7d,
        DENSE_RANK() OVER(
            PARTITION BY t.dt_report
            ORDER BY t.count_prev_7d DESC
            ) AS rank_count_prev_7d
    FROM (
        SELECT
            dt_report,
            login_hash,
            server_hash,
            symbol,
            currency,
            SUM(volume) OVER (
                PARTITION BY login_hash, symbol
                ORDER BY dt_report
                RANGE BETWEEN '7 days' PRECEDING AND CURRENT ROW
            ) AS volume_7d,
            Count(*) OVER (
                PARTITION BY login_hash
                ORDER BY dt_report
                RANGE BETWEEN '7 days' PRECEDING AND CURRENT ROW
            ) AS count_prev_7d
        FROM merged_trades
    ) AS t
), f3 as (
    -- sum_volume_2020_08
    select
        login_hash,
        server_hash,
        symbol,
        currency,
        sum(volume) as sum_volume_2020_08
    from merged_trades
    where dt_report between '2020-08-01' and '2020-08-31'
    group by login_hash, server_hash, symbol, currency
), final_solution  as (

    select f1.dt_report,
           f1.login_hash,
           f1.server_hash,
           f1.symbol,
           f1.currency,
           f1.sum_volume_prev_7d,
           f1.sum_volume_prev_all,
           f2.rank_volume_symbol_prev_7d,
           f2.rank_count_prev_7d,
           f3.sum_volume_2020_08,
           f1.date_first_trade,
           f1.row_number
    from f1
        left outer join f2
        on f2.dt_report = f1.dt_report and
           f2.login_hash = f1.login_hash and
           f2.server_hash = f1.server_hash and
           f2.symbol = f1.symbol and
           f2.currency = f1.currency
        left outer join f3
        on f3.login_hash = f1.login_hash and
           f3.server_hash = f1.server_hash and
           f3.symbol = f1.symbol and
           f3.currency = f1.currency
    where f1.dt_report between '2020-06-01' and '2020-08-31'
    order by row_number desc
)

select * from final_solution;