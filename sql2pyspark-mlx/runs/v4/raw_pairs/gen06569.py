from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w4 = Window.partitionBy("product_id").orderBy(F.col("unit_price_percent_rank").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("least_quantity_unit_price_per_running_quantity", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy("least_quantity_cume_dist_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("quantity_cume_dist", "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

scored = (
    df_order_items
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w))
    .withColumn("running_quantity", F.sum("quantity").over(w2))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w3))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

cleaned = (
    latest
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price_percent_rank",
        "quantity_cume_dist",
        F.round((F.col("unit_price") / F.col("running_quantity")), 2).alias("unit_price_per_running_quantity"),
    )
)

filtered = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .withColumn("least_quantity_cume_dist_quantity", F.least("quantity_cume_dist", "quantity"))
    .withColumn("least_quantity_unit_price_per_running_quantity", F.least("quantity", "unit_price_per_running_quantity"))
)

enriched = (
    filtered
    .withColumn("least_quantity_unit_price_per_running_quantity_avg_2_prior", F.avg("least_quantity_unit_price_per_running_quantity").over(w5))
    .withColumn("product_id_stddev_samp_least_quantity_unit_price_per_running_quantity", F.stddev_samp("least_quantity_unit_price_per_running_quantity").over(w6))
    .withColumn("product_id_max_unit_price_percent_rank", F.max("unit_price_percent_rank").over(w6))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w7))
    .withColumn("quantity_cume_dist_bucket", F.ntile(4).over(w8))
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price_percent_rank",
        "unit_price_per_running_quantity",
        "least_quantity_cume_dist_quantity",
        "least_quantity_unit_price_per_running_quantity",
        "product_id_max_unit_price_percent_rank",
        "first_known_quantity",
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("unit_price_per_running_quantity").desc())
)
