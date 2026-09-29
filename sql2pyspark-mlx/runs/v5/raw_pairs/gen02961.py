from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("last3_avg_unit_price").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

scored = (
    filtered
    .filter(F.col("quantity") <= 5)
    .withColumn("last3_sum_unit_price", F.sum("unit_price").over(w))
    .withColumn("last3_avg_unit_price", F.avg("unit_price").over(w))
    .withColumn("last3_count_unit_price", F.count("unit_price").over(w))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "quantity",
        "last3_sum_unit_price",
        "last3_avg_unit_price",
        "last3_count_unit_price",
        "rn",
    )
    .orderBy(F.col("last3_sum_unit_price").desc())
)
