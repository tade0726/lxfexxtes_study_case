"""

Task 1: Data quality check

Task: Show the code you used to run the tests you performed to identify the quality control 
issues we have intentionally introduced into this data. And share any conclusions you have arrived at as comments.


- Please look for unexpected strings, unexpected numerical values, and unexpected dates.

- Please check that any joins you need to make to ensure data integrity.

- Please test any edge cases you think should be investigated to produce the highest quality
possible.


"""

from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.orm import sessionmaker, declarative_base

import pandas as pd

import os
from dotenv import load_dotenv

from pprint import pprint


# Load environment variables from .env file, one should recreate the files from .env.example template
load_dotenv()

# Database connection settings
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Create database URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

Base = declarative_base()


def checking_users_tables():

    checkers = {}

    with engine.connect() as conn:

        # vertify combination of login_hash and server_hash is unique
        # YES, 666 is the total numbers of unique combinations and unique records

        SQL_TEXT = """
        select count(*) from (select distinct * from users) as t1
        UNION ALL
        select count(*) from (select distinct login_hash, server_hash from users) as t2;
        """

        counts = conn.execute(text(SQL_TEXT)).fetchall()

        if counts[0][0] != counts[1][0]:
            checkers["unique_combinations"] = (
                False,
                {
                    "unique_combinations": counts[0][0],
                    "unique_records": counts[1][0],
                },
            )
        else:
            checkers["unique_combinations"] = (True, {})

        # checking server_hash, how many unique users per server
        server_hash_counts = conn.execute(
            text(
                """
                SELECT server_hash, COUNT(DISTINCT login_hash) as unique_users
                FROM users 
                GROUP BY server_hash
                ORDER BY unique_users DESC;
            """
            )
        ).fetchall()

        checkers["server_hash_users"] = {row[0]: row[1] for row in server_hash_counts}

        # checking country_hash, unique user count per country

        country_hash_counts = conn.execute(
            text(
                """
                SELECT country_hash, COUNT(DISTINCT login_hash) as unique_users
                FROM users 
                GROUP BY country_hash
                ORDER BY unique_users DESC;
            """
            )
        ).fetchall()

        checkers["country_hash_users"] = {row[0]: row[1] for row in country_hash_counts}

        # checking currency string - find how many unqiue users per currency

        currency_counts = conn.execute(
            text(
                """
                SELECT currency, COUNT(DISTINCT login_hash) as unique_users
                FROM users 
                GROUP BY currency
                ORDER BY unique_users DESC;
            """
            )
        ).fetchall()

        checkers["currency_users"] = {row[0]: row[1] for row in currency_counts}

        # checking currency string - make sure 1 to 1 with country_hash

        country_currency_mapping = conn.execute(
            text(
                """
                WITH CountryCurrencyPairs AS (
                    SELECT 
                        country_hash,
                        COUNT(DISTINCT currency) as currency_count,
                        array_agg(DISTINCT currency) as currencies
                    FROM users
                    GROUP BY country_hash
                    HAVING COUNT(DISTINCT currency) > 1
                )
                SELECT 
                    country_hash,
                    currency_count,
                    currencies
                FROM CountryCurrencyPairs
                ORDER BY currency_count DESC;
            """
            )
        ).fetchall()

        checkers["country_currency_mapping"] = {
            "is_one_to_one": len(country_currency_mapping) == 0,
            "violations": {
                row[0]: {"currency_count": row[1], "currencies": row[2]}
                for row in country_currency_mapping
            },
        }

        # checking how many unique values in currency column
        unique_values = conn.execute(
            text(
                """
                SELECT 
                    DISTINCT currency
                FROM users;
            """
            )
        ).fetchall()

        if "unique_values" not in checkers:
            checkers["unique_values"] = {}

        checkers["unique_values"]["currency"] = [row[0] for row in unique_values]

        # checking `enable` columns unique values
        unique_values = conn.execute(
            text(
                """
                SELECT 
                    DISTINCT enable
                FROM users;
            """
            )
        ).fetchall()

        if "unique_values" not in checkers:
            checkers["unique_values"] = {}

        checkers["unique_values"]["enable"] = [row[0] for row in unique_values]

        # checking login_hash + server_hash + country_hash is unique
        # To check if the combination of login_hash, server_hash, and country_hash is unique

        unique_values_counts = conn.execute(
            text(
                """
                SELECT count(*) from (
                    SELECT 
                        DISTINCT login_hash,
                        server_hash,
                        country_hash
                    FROM users
                ) as unique_values;
            """
            )
        ).fetchall()[0]

        records_count = conn.execute(
            text(
                """
                SELECT 
                    COUNT(*)
                FROM users;
            """
            )
        ).fetchone()[0]

        checkers["login_hash_server_hash_country_hash_unique"] = (
            records_count == unique_values_counts
        )

        return checkers


def checking_trades_tables():

    checkers = {}

    with engine.connect() as conn:

        # checking the combination of `login_hash` and `server_hash` and `ticket_hash` is unique

        SQL = """
        select count(*) from (select distinct ticket_hash, login_hash, server_hash from trades) t1
        UNION ALL
        select count(*) from (select distinct * from trades) t2;
        """

        counts = conn.execute(text(SQL)).fetchall()
        checkers["login_hash_server_hash_ticket_hash_unique"] = (
            counts[0][0] == counts[1][0]
        )

        if False:
            # show unique values of `symbol` column
            unique_values = conn.execute(
                text(
                    """
                    SELECT DISTINCT symbol
                    FROM trades;
                """
                )
            ).fetchall()

            checkers["symbol_unique"] = [row[0] for row in unique_values]

        # checking `symbol`, group by different length
        # more of the symbols are length or 6

        symbols = conn.execute(
            text(
                """
                SELECT 
                    DISTINCT symbol
                FROM trades
            """
            )
        ).fetchall()

        s_symbols = pd.Series([row[0] for row in symbols])

        checkers["symbol_length_unique"] = s_symbols.apply(len).value_counts().to_dict()

        # checking `symbol`, pure alphabetic characters or having numbers or having special characters
        def categorize_symbol(symbol):
            is_pure_alpha = symbol.isalpha()
            has_digit = any(c.isdigit() for c in symbol)
            has_special = not all(c.isalnum() for c in symbol)
            return is_pure_alpha, has_digit, has_special

        symbol_categories = s_symbols.apply(categorize_symbol)

        checkers["symbol_composition"] = {
            "pure_alphabetic": sum(cat[0] for cat in symbol_categories),
            "contains_numbers": sum(cat[1] for cat in symbol_categories),
            "contains_special": sum(cat[2] for cat in symbol_categories),
        }

        # checking `digits` column
        ## any outliers, using 3 standard deviations and IQR

        s_digits = pd.Series(
            [
                row[0]
                for row in conn.execute(text("SELECT digits FROM trades;")).fetchall()
            ]
        )

        checkers["digits_outliers"] = check_outliers(s_digits)

        # checking `cmd` column, distribution of values, dinstinct counts of unique `ticket_hash`
        unique_values = conn.execute(
            text(
                """
                SELECT 
                    cmd, COUNT(DISTINCT ticket_hash)
                FROM trades
                group by cmd;
                """
            )
        ).fetchall()

        checkers["cmd_distinct_ticket_hash_counts"] = {
            row[0]: row[1] for row in unique_values
        }

        # checking `volumn` column
        ## any outliers, using 3 standard deviations and IQR

        s_volume = pd.Series(
            [
                row[0]
                for row in conn.execute(text("SELECT volume FROM trades;")).fetchall()
            ]
        )

        checkers["volume_outliers"] = check_outliers(s_volume, outliers=False)

        # checking `open_time` column and `close_time` column, check if close time is after open time
        open_time = pd.Series(
            [
                row[0]
                for row in conn.execute(
                    text("SELECT open_time FROM trades;")
                ).fetchall()
            ]
        )

        close_time = pd.Series(
            [
                row[0]
                for row in conn.execute(
                    text("SELECT close_time FROM trades;")
                ).fetchall()
            ]
        )

        checkers["open_time_close_time_reversed_counts"] = (
            open_time > close_time
        ).sum()

        # checking duration between `open_time` and `close_time`

        duration = conn.execute(
            text(
                """
                WITH c1 AS (
                    SELECT 
                        trades.open_time,
                        trades.close_time,
                        (trades.close_time - trades.open_time) AS duration 
                    FROM trades
                )
                SELECT *
                FROM c1
                WHERE duration > '720 day'
                """
            )
        ).fetchall()

        checkers["over_720_days_duration_trades_counts"] = len(duration)

        # checking `price` and `contractsize` for distribution

        s_price = pd.Series(
            [
                row[0]
                for row in conn.execute(
                    text("SELECT open_price FROM trades;")
                ).fetchall()
            ]
        )

        s_contractsize = pd.Series(
            [
                row[0]
                for row in conn.execute(
                    text("SELECT contractsize FROM trades;")
                ).fetchall()
            ]
        )

        checkers["price_dist"] = check_outliers(s_price, decimals=3, outliers=False)
        checkers["contractsize_dist"] = check_outliers(
            s_contractsize, decimals=3, outliers=False
        )

        return checkers


def checking_data_integrity() -> dict:
    """
    Check data integrity between users and trades tables by validating login_hash relationships.

    Returns:
        dict: Dictionary containing integrity check results with counts of mismatched login_hashes
    """
    integrity_results = {}

    with engine.connect() as conn:
        # Check for login_hash values that don't exist in both tables using FULL OUTER JOIN
        missing_login_hashes_query = text(
            """
            WITH login_hash_comparison AS (
                SELECT 
                    users.login_hash AS users_login_hash,
                    trades.login_hash AS trades_login_hash
                FROM users
                FULL OUTER JOIN trades 
                    ON users.login_hash = trades.login_hash
            )
            SELECT COUNT(*) 
            FROM login_hash_comparison
            WHERE users_login_hash IS NULL 
                OR trades_login_hash IS NULL;
        """
        )

        result = conn.execute(missing_login_hashes_query)
        integrity_results["missing_login_hashes_count"] = result.scalar()

        return integrity_results


def check_outliers(series: pd.Series, decimals: int = 3, outliers: bool = True) -> dict:
    """
    Check for outliers in a pandas Series using both standard deviation and IQR methods.

    Args:
        series: pandas Series containing numerical data
        decimals: number of decimal places to round to (default: 3)

    Returns:
        dict containing outliers and statistics
    """
    # Calculate standard deviation statistics
    mean = series.mean()
    std = series.std()

    # Calculate IQR statistics
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    iqr_lower_bound = Q1 - 1.5 * IQR
    iqr_upper_bound = Q3 + 1.5 * IQR

    result = {
        "std_outliers": [
            round(x, decimals)
            for x in series[
                (series < mean - 3 * std) | (series > mean + 3 * std)
            ].tolist()
        ],
        "iqr_outliers": [
            round(x, decimals)
            for x in series[
                (series < iqr_lower_bound) | (series > iqr_upper_bound)
            ].tolist()
        ],
        "mean": round(mean, decimals),
        "std": round(std, decimals),
        "min": round(series.min(), decimals),
        "max": round(series.max(), decimals),
        "q1": round(Q1, decimals),
        "q3": round(Q3, decimals),
        "iqr": round(IQR, decimals),
        "iqr_bounds": {
            "lower": round(iqr_lower_bound, decimals),
            "upper": round(iqr_upper_bound, decimals),
        },
    }

    if not outliers:
        del result["std_outliers"]
        del result["iqr_outliers"]

    return result


if __name__ == "__main__":

    print("\nChecking users table: \n")
    user_table_checker = checking_users_tables()
    pprint(user_table_checker)

    print("=" * 80)

    print("\nChecking trades table: \n")
    trades_table_checker = checking_trades_tables()
    pprint(trades_table_checker)

    print("=" * 80)

    print("\nChecking data integrity: \n")
    data_integrity_checkers = checking_data_integrity()
    pprint(data_integrity_checkers)
