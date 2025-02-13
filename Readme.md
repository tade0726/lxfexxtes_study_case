# Study case : LifeBytes


## Pre-requisites

- uv: for lib management and python environment creation
- Python 3.12

## Task 1: Data quality check

Output: tech_test_qa_{your_name}.py

Task: Show the code you used to run the tests you performed to identify the quality control
issues we have intentionally introduced into this data. And share any conclusions you have
arrived at as comments.

- Please look for unexpected strings, unexpected numerical values, and unexpected dates.

- Please check that any joins you need to make to ensure data integrity.

- Please test any edge cases you think should be investigated to produce the highest quality
possible.



To get the results, you can use the following command:
```shell
uv run  tech_test_qa_{your_name}.py
```


## Task 2: Business Report

Output: tech_test_query_{your_name}.sql

Task: You are requested to create a query that satisfies the below requirements.

Return a row for every combination of dt_report/login/server/symbol/currency every day in June, July, August and September 2020.

Your method should work even if there is no data on a particular day in this period within the data, and you can report based on close_time.

Please run this query on users that exist in the user's table only.

Please include enabled accounts only.

Please return the data in order of row_number in descending.

Please fix any quality control issues you identify in the first requirements within this query.



## Tables:




# Database Schema

## Tables Overview

| Table Name | Description |
|------------|-------------|
| users      | Stores user information including login hash, server hash, country hash and currency |
| trades     | Contains trading information including ticket hash, symbol, volume, and pricing data |

## Detailed Schema

### Users Table

| Column | Type | Description |
|--------|------|-------------|
| login_hash | TEXT | hashed user login ID |
| server_hash | TEXT | hashed machine ID (note that logins and tickets belong to servers) |
| country_hash | TEXT | hash of the country of the user |
| currency | TEXT | denomination of the account currency |
| enable | BOOLEAN | if the login account is enabled or not |

### Trades Table

| Column | Type | Description |
|--------|------|-------------|
| ticket_hash | TEXT | hashed trade ID |
| login_hash | TEXT | hashed user login ID |
| server_hash | TEXT | hashed machine ID (note that logins and tickets belong to servers) |
| symbol | TEXT | financial instrument being traded |
| digits | INTEGER | number of significant digits after the decimal place |
| cmd | INTEGER | 0 = buy, 1 = sell |
| volume | FLOAT | size of the trade |
| open_time | TIMESTAMP | open time of the trade |
| open_price | FLOAT | opening price of the trade |
| close_time | TIMESTAMP | close time of the trade (epoch means trade still open) |
| contractsize | FLOAT | size of a single contract of the financial instrument |

## Indexes

### Users Table
- PRIMARY KEY on `login_hash`
- Index on `country_hash`

### Trades Table
- PRIMARY KEY on `ticket_hash`
- Index on `login_hash` (foreign key)
- Index on `symbol`
- Index on `open_time`
