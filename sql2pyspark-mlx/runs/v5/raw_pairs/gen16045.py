from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("balance_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy("balance_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type", "status").orderBy("balance_cume_dist", "id")
w6 = Window.partitionBy("account_type").orderBy("prev_first_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_type", "customer_id").orderBy("next_balance_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_type", "customer_id").orderBy("first_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & (~F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select("*", F.round((F.col("balance") * 1.07), 2).alias("balance_scaled"))
)

metrics = (
    valid
    .filter(F.col("status") == "OPEN")
    .withColumn("balance_cume_dist", F.cume_dist().over(w_part))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w2))
)

enriched = (
    metrics
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        "balance_scaled",
        "balance_cume_dist",
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status"),
        F.first("balance", ignorenulls=True).over(w4).alias("first_known_balance"),
    )
)

enriched_accounts = (
    enriched
    .withColumn("next_balance_scaled", F.lead("balance_scaled").over(w5))
    .withColumn("prev_balance_scaled", F.lag("balance_scaled").over(w5))
    .withColumn("next_first_known_balance", F.lead("first_known_balance").over(w5))
    .withColumn("prev_first_known_balance", F.lag("first_known_balance").over(w5))
    .withColumn("row_seq", F.row_number().over(w5))
)

eligible = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.is_flagged")), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "customer_id",
        "status",
        "balance_scaled",
        "balance_cume_dist",
        "first_known_status",
        "first_known_balance",
        "prev_balance_scaled",
        "prev_first_known_balance",
        F.sum("prev_balance_scaled").over(w6).alias("cumulative_prev_balance_scaled"),
        F.sum("next_balance_scaled").over(w7).alias("cumulative_next_balance_scaled"),
        F.first("prev_balance_scaled", ignorenulls=True).over(w8).alias("first_known_prev_balance_scaled"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["city", "segment", "status"], ["city"], []],
        F.col("c.city").alias("city"),
        F.col("c.segment").alias("segment"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.first_known_balance")).alias("min_first_known_balance"),
        F.grouping_id().alias("gid"),
    )
)
