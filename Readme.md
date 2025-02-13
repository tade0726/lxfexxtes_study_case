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




## Tables:




# Database Schema

## Tables Overview

| Table Name | Description |
|------------|-------------|
| users      | Stores user information including login hash, server hash, country hash and currency |
| trades     | Contains trading information including ticket hash, symbol, volume, and pricing data |

## Detailed Schema

### Users Table

| Column        | Type    | Description |
|--------------|---------|-------------|
| login_hash   | text    | Unique user identifier hash |
| server_hash  | text    | Server hash identifier |
| country_hash | text    | Hash of the user's country |
| currency     | text    | Account currency denomination |
| enable       | bigint  | Account status flag |

### Trades Table

| Column       | Type             | Description |
|-------------|------------------|-------------|
| login_hash  | text            | Reference to user's login hash |
| ticket_hash | text            | Unique trade identifier hash |
| server_hash | text            | Server hash identifier |
| symbol      | text            | Trading symbol/instrument |
| digits      | bigint          | Decimal precision for the instrument |
| cmd         | bigint          | Trade command/type |
| volume      | bigint          | Trade volume |
| open_time   | timestamp       | Trade opening time |
| open_price  | double precision| Opening price |
| close_time  | timestamp       | Trade closing time |
| contractsize| double precision| Contract size for the trade |

## Indexes

### Users Table
- PRIMARY KEY on `login_hash`
- Index on `country_hash`

### Trades Table
- PRIMARY KEY on `ticket_hash`
- Index on `login_hash` (foreign key)
- Index on `symbol`
- Index on `open_time`
