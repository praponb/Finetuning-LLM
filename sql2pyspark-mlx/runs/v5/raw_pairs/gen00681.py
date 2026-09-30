from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("last4_sum_inventory_quantity").asc_nulls_first())
w4 = Window.partitionBy("region").orderBy("last4_max_inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("region").orderBy("running_last4_count_inventory_quantity", "id")
w7 = Window.partitionBy("region").orderBy(F.col("last4_max_inventory_quantity").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_windows = (
    latest_match
    .select(
        "id",
        "region",
        "opened_date",
        "inventory_last_restock",
        F.max("inventory_quantity").over(w2).alias("last4_max_inventory_quantity"),
        F.count("inventory_quantity").over(w2).alias("last4_count_inventory_quantity"),
        F.sum("inventory_quantity").over(w2).alias("last4_sum_inventory_quantity"),
    )
)

enriched = (
    with_windows
    .filter(F.col("last4_count_inventory_quantity").isNotNull())
    .select(
        "id",
        "region",
        "opened_date",
        "last4_max_inventory_quantity",
        "last4_sum_inventory_quantity",
        F.dense_rank().over(w3).alias("last4_sum_inventory_quantity_dense_rank"),
        F.sum("last4_count_inventory_quantity").over(w4).alias("running_last4_count_inventory_quantity"),
        F.count("last4_max_inventory_quantity").over(w5).alias("region_count_last4_max_inventory_quantity"),
    )
)

metrics = (
    enriched
    .withColumn("region_stddev_samp_last4_max_inventory_quantity", F.stddev_samp("last4_max_inventory_quantity").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("last4_max_inventory_quantity_bucket", F.ntile(5).over(w7))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.last4_max_inventory_quantity",
        "s.last4_sum_inventory_quantity",
        "s.running_last4_count_inventory_quantity",
        "s.region_count_last4_max_inventory_quantity",
        "s.last4_max_inventory_quantity_bucket",
        F.col("c.units"),
        F.col("c.sale_date"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.last4_max_inventory_quantity",
        "s.last4_sum_inventory_quantity",
        "s.running_last4_count_inventory_quantity",
        "s.last4_max_inventory_quantity_bucket",
        "s.units",
        "s.sale_date",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "region",
        "last4_max_inventory_quantity",
        "last4_sum_inventory_quantity",
    )
    .unpivot(
        ids=["id", "region"],
        values=["last4_max_inventory_quantity", "last4_sum_inventory_quantity"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
