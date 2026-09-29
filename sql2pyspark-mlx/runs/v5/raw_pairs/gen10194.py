from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("product_id").orderBy(F.col("days_since_last_restock").asc_nulls_first())
w3 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("category").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("days_since_last_restock_rank").asc_nulls_last(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-06-16").cast("date"))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.when(F.col("quantity") >= 240, "High").when(F.col("quantity") >= 60, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .select(
        "src.*",
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("p.product_name"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.stddev_samp("days_since_last_restock").over(w).alias("category_stddev_samp_days_since_last_restock"),
        F.rank().over(w2).alias("days_since_last_restock_rank"),
        F.last("days_since_last_restock", ignorenulls=True).over(w3).alias("last_known_days_since_last_restock"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.avg("days_since_last_restock_rank").over(w_recent).alias("last3_avg_days_since_last_restock_rank"),
        F.max("days_since_last_restock_rank").over(w_recent).alias("last3_max_days_since_last_restock_rank"),
        F.count("days_since_last_restock_rank").over(w_recent).alias("last3_count_days_since_last_restock_rank"),
        F.sum("days_since_last_restock_rank").over(w_recent).alias("last3_sum_days_since_last_restock_rank"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "days_since_last_restock",
        "category",
        "product_name",
        "category_stddev_samp_days_since_last_restock",
        "days_since_last_restock_rank",
        "last3_avg_days_since_last_restock_rank",
        "last3_max_days_since_last_restock_rank",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("last3_avg_days_since_last_restock_rank").desc_nulls_first())
)
