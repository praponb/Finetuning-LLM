from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w_hist = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy(F.col("greatest_quantity_cume_dist_quantity_sum_7d_avg_7d").desc())

remaining_keys = (
    df_inventory
    .select("id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2023-03-01", "2025-07-01"))
        .select("id", "product_id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("last_restock") >= F.lit("2023-05-10").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 34).alias("last_restock_plus_34d"),
    )
)

cleaned = (
    valid
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-08-11"))) & F.col("quantity").between(86, 145))
    .withColumn("last_restock_period", F.date_trunc("week", "last_restock"))
    .withColumn("last_restock_plus_34d_month_end", F.last_day("last_restock_plus_34d"))
)

with_windows = (
    cleaned
    .filter(F.col("quantity") != 192)
    .select(
        "id",
        "last_restock_period",
        "last_restock_plus_34d_month_end",
        F.cume_dist().over(w_running).alias("quantity_cume_dist"),
        F.sum("quantity").over(w_hist).alias("quantity_sum_7d"),
        F.sum("quantity").over(w3).alias("quantity_sum_90d"),
    )
)

filtered = (
    with_windows
    .filter(F.month("last_restock_plus_34d_month_end").isin([8, 9, 12]))
    .select(
        "id",
        "last_restock_period",
        "last_restock_plus_34d_month_end",
        "quantity_sum_7d",
        "quantity_sum_90d",
        (F.col("quantity_cume_dist") - F.col("quantity_sum_90d")).alias("quantity_cume_dist_minus_quantity_sum_90d"),
        F.greatest("quantity_cume_dist", "quantity_sum_7d").alias("greatest_quantity_cume_dist_quantity_sum_7d"),
    )
)

metrics = (
    filtered
    .withColumn("quantity_cume_dist_minus_quantity_sum_90d_avg_30d", F.avg("quantity_cume_dist_minus_quantity_sum_90d").over(w_ordered))
    .withColumn("greatest_quantity_cume_dist_quantity_sum_7d_count_30d", F.count("greatest_quantity_cume_dist_quantity_sum_7d").over(w_ordered))
    .withColumn("greatest_quantity_cume_dist_quantity_sum_7d_avg_7d", F.avg("greatest_quantity_cume_dist_quantity_sum_7d").over(w5))
    .withColumn("quantity_cume_dist_minus_quantity_sum_90d_sum_30d", F.sum("quantity_cume_dist_minus_quantity_sum_90d").over(w_ordered))
)

enriched = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w6).alias("greatest_quantity_cume_dist_quantity_sum_7d_avg_7d_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "last_restock_plus_34d_month_end",
        "quantity_sum_7d",
        "quantity_sum_90d",
        "quantity_cume_dist_minus_quantity_sum_90d",
        "quantity_cume_dist_minus_quantity_sum_90d_avg_30d",
        "greatest_quantity_cume_dist_quantity_sum_7d_count_30d",
        "greatest_quantity_cume_dist_quantity_sum_7d_avg_7d",
        "quantity_cume_dist_minus_quantity_sum_90d_sum_30d",
        "greatest_quantity_cume_dist_quantity_sum_7d_avg_7d_cume_dist",
    )
)
