from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .withColumn("balance_cume_dist", F.cume_dist().over(w))
    .withColumn("balance_avg_6", F.avg("balance").over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
)

result = (
    enriched
    .cube(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("balance") >= 89000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "account_type",
    )
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.percentile_approx("balance_cume_dist", [0.25, 0.5, 0.75]).alias("balance_cume_dist_quartiles"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("distinct_customer_ids") > 1)
)
