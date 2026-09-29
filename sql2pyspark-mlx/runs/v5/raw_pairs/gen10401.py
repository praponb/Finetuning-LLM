from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id", "status").orderBy("balance_filled", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("customer_id").orderBy("last3_max_balance", "id")
w4 = Window.partitionBy("customer_id").orderBy("balance_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("last3_min_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("running_last3_avg_balance").desc(), "id")

cleaned = (
    df_accounts
    .filter(~F.col("status").eqNullSafe(F.lit("OPEN")))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

metrics = (
    cleaned
    .withColumn("last3_count_balance", F.count("balance").over(w_ordered))
    .withColumn("last3_min_balance", F.min("balance").over(w_ordered))
    .withColumn("last3_avg_balance", F.avg("balance").over(w_ordered))
    .withColumn("last3_max_balance", F.max("balance").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_part))
)

enriched = (
    metrics
    .select(
        "id",
        "customer_id",
        "balance",
        "balance_filled",
        "last3_avg_balance",
        "last3_max_balance",
        F.lead("last3_min_balance", 1).over(w3).alias("next_last3_min_balance"),
        F.sum("last3_avg_balance").over(w4).alias("running_last3_avg_balance"),
        F.last("last3_max_balance", ignorenulls=True).over(w5).alias("last_known_last3_max_balance"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "last3_max_balance",
        "next_last3_min_balance",
        "running_last3_avg_balance",
        "rank_in_group",
        F.when(F.col("last_known_last3_max_balance") >= 71000, "High").when(F.col("last_known_last3_max_balance") >= 13000, "Medium").otherwise("Low").alias("last_known_last3_max_balance_tier"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "balance",
        "running_last3_avg_balance",
        "rank_in_group",
        "last_known_last3_max_balance_tier",
    )
)
