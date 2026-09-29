from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("order_date").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.orderBy("quantity", "id")

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

ranked = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_name", "city")
)

eligible = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date").between("2023-07-26", "2023-12-01")), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", F.col("c.quantity"))
)

scored = (
    with_last
    .select(
        "*",
        F.lead("quantity", 1).over(w4).alias("next_quantity"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

cleaned = (
    scored
    .filter(F.col("next_quantity") <= 100)
    .select(
        "*",
        F.least("quantity", "next_quantity").alias("least_quantity_next_quantity"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock").between("2023-07-04", "2025-01-01")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("next_quantity") >= 237, "LARGE").otherwise("SMALL").alias("next_quantity_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("least_quantity_next_quantity", [0.25, 0.5, 0.75]).alias("least_quantity_next_quantity_quartiles"),
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
