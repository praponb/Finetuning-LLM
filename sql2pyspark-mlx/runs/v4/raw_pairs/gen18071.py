from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date_month_end").desc_nulls_first(), "id")
w_hist = Window.partitionBy("customer_id").orderBy("opened_date_month_end", "id")
w_recent = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_last())
w5 = Window.orderBy("last_known_balance", "id")
w6 = Window.partitionBy("status").orderBy(F.col("balance_percent_rank").asc_nulls_last())

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .withColumn("next_balance", F.lead("balance", 2).over(w_hist))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w_recent))
    .withColumn("balance_percent_rank", F.percent_rank().over(w4))
)

result = (
    enriched
    .select(
        "*",
        F.lead("balance", 1).over(w5).alias("next_balance_2"),
        F.rank().over(w6).alias("balance_percent_rank_rank"),
    )
)
