from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").asc_nulls_last(), "id")

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_anti")
)

deduped = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "opened_date", "status")
)

valid = (
    deduped
    .filter((F.col("opened_date") > F.lit("2023-08-23").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date_period")), "left_semi")
)

result = (
    kept
    .groupBy("status")
    .agg(
        ((F.unix_timestamp(F.max("opened_date_period")) - F.unix_timestamp(F.min("opened_date_period"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("opened_date_period")) - F.unix_timestamp(F.min("opened_date_period"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("opened_date_period")) - F.unix_timestamp(F.min("opened_date_period"))) / 60).alias("duration_min_2"),
    )
    .orderBy(F.col("duration_min_2").desc_nulls_first())
)
