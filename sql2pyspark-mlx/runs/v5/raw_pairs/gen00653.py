from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_hist = Window.orderBy("inventory_quantity", "id")
w4 = Window.orderBy(F.col("last_restock").desc(), "id")
w_part = Window.orderBy(F.col("running_avg_sqft").desc(), "id")
w6 = Window.orderBy(F.col("inventory_quantity_bucket_2").asc_nulls_last())
w7 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.upper("store_name").alias("store_name_upper"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "sqft",
        "store_name_upper",
        F.min("sqft").over(w_recent).alias("running_min_sqft"),
        F.max("sqft").over(w_recent).alias("running_max_sqft"),
        F.avg("sqft").over(w_recent).alias("running_avg_sqft"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name_upper",
        "s.running_avg_sqft",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "store_name_upper",
        "last_restock",
        F.ntile(10).over(w_hist).alias("inventory_quantity_bucket"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(3).over(w_hist).alias("inventory_quantity_bucket_2"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "inventory_quantity_bucket",
        F.dense_rank().over(w6).alias("inventory_quantity_bucket_2_dense_rank"),
        F.sum("seq_no").over(w7).alias("cumulative_seq_no"),
        F.avg("row_seq").over(w8).alias("row_seq_avg_5"),
        F.sum("inventory_quantity_bucket").over(w9).alias("running_inventory_quantity_bucket"),
    )
)

with_last_stores = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

result = (
    with_last_stores
    .select(
        "id",
        "inventory_quantity_bucket_2_dense_rank",
        "cumulative_seq_no",
        "row_seq_avg_5",
        "running_inventory_quantity_bucket",
    )
)
