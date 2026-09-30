from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w_part = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("next_balance_tier").orderBy(F.col("avg_next_balance_2").desc())

enriched = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-02-01")
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.lead("balance", 1).over(w_part).alias("next_balance_2"),
        F.sum("balance").over(w3).alias("cumulative_balance"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("next_balance") >= 77000, "PREMIUM").otherwise("STANDARD").alias("next_balance_tier"),
        F.date_trunc("year", "opened_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("next_balance", 0.5).alias("next_balance_p50"),
        F.avg("next_balance_2").alias("avg_next_balance_2"),
    )
    .withColumn("avg_next_balance_2_percent_rank", F.percent_rank().over(w4))
)
