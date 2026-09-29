from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "quantity", "unit_price")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.when(F.col("unit_price") >= 380, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("quantity").alias("total_quantity"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
