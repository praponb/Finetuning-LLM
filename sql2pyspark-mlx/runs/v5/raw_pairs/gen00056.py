from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy("is_closed", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("balance_tier", F.when(F.col("balance") >= 78000, "High").when(F.col("balance") >= 26000, "Medium").otherwise("Low"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

base = (
    eligible
    .filter((F.col("opened_date") > F.lit("2023-09-11").cast("date")) & (F.col("account_type") != "CREDIT"))
    .select("*", F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"))
)

enriched = (
    base
    .select(
        "id",
        "account_type",
        "opened_date",
        F.ntile(10).over(w).alias("balance_bucket"),
        F.sum("is_closed").over(w2).alias("running_is_closed"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_is_closed") >= 371, "LARGE").otherwise("SMALL").alias("running_is_closed_tier"),
    )
    .agg(
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.round(F.avg("running_is_closed"), 2).alias("avg_running_is_closed"),
        F.round(F.avg("balance_bucket"), 2).alias("avg_balance_bucket"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.percentile_approx("running_is_closed", [0.25, 0.5, 0.75]).alias("running_is_closed_quartiles"),
    )
)
