from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date_period", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-11-24").cast("date")) & (F.col("status") == "CLOSED"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.upper("status").alias("status_upper"),
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_prev = ranked.withColumn("prev_opened_date_period", F.lag("opened_date_period").over(w2))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_opened_date_period").isNull() | (F.col("opened_date_period") > F.col("prev_opened_date_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_accounts = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w3))

result = (
    sessions_accounts
    .rollup(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        ((F.unix_timestamp(F.max("opened_date_period")) - F.unix_timestamp(F.min("opened_date_period"))) / 60).alias("duration_min"),
        F.count("*").alias("cnt"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
    )
)
