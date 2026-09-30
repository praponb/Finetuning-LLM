from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")

deduped = (
    df_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)
