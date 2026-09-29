from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id")

deduped = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.when(F.col("balance") >= 70000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
    )
    .filter(F.col("balance_p50") > 0)
    .orderBy(F.col("savings_events").desc_nulls_last())
)
