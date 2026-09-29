from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity_rounded", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("next_quantity").desc())
w5 = Window.partitionBy("product_id").orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_restock", "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity_rounded").desc(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

valid = (
    df_inventory
    .filter(F.col("quantity") >= 159)
    .select(
        "*",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

metrics = (
    valid
    .withColumn("prev_quantity_rounded", F.lag("quantity_rounded", 1).over(w))
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w2))
    .withColumn("quantity_scaled_avg_5", F.avg("quantity_scaled").over(w3))
)

latest = (
    metrics
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

metrics_inventory = (
    latest
    .select(
        "*",
        F.last("quantity_scaled_avg_5", ignorenulls=True).over(w5).alias("last_known_quantity_scaled_avg_5"),
        F.sum("quantity_scaled_avg_5").over(w6).alias("running_quantity_scaled_avg_5"),
        F.row_number().over(w7).alias("seq_no"),
        F.ntile(10).over(w8).alias("prev_quantity_rounded_bucket"),
    )
)

base = (
    metrics_inventory
    .filter(F.col("last_restock") >= F.lit("2023-09-16").cast("date"))
    .select(
        "*",
        (F.col("seq_no") / F.nullif("quantity_rounded", F.lit(0))).alias("seq_no_per_quantity_rounded"),
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)

result = (
    base
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_scaled",
        "quantity_rounded",
        "prev_quantity_rounded",
        "quantity_scaled_avg_5",
        "rn",
        "seq_no",
        F.col("max_store_sale_units"),
    )
    .orderBy(F.col("id").desc())
)
