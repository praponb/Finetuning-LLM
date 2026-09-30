from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy(F.col("corr_quantity_unit_price").desc())
w4 = Window.orderBy("corr_quantity_unit_price", "id")
w5 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w6 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w7 = Window.partitionBy("order_id").orderBy("order_id_avg_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy(F.col("next_quantity").desc_nulls_last())
w9 = Window.partitionBy("product_id", "order_id")
w10 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(-5, -1)

enriched = (
    df_order_items
    .withColumn("quantity_bucket", F.ntile(3).over(w))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w2))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    deduped
    .withColumn("corr_quantity_unit_price_bucket", F.ntile(5).over(w4))
    .withColumn("next_quantity", F.lead("quantity", 2, 0).over(w5))
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w2))
    .withColumn("seq_no", F.row_number().over(w6))
)

scored = (
    metrics
    .withColumn("first_known_quantity_bucket", F.first("quantity_bucket", ignorenulls=True).over(w7))
    .withColumn("next_quantity_dense_rank", F.dense_rank().over(w8))
    .withColumn("quantity_bucket_share", F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w9)), 4))
    .withColumn("quantity_bucket_avg_5_prior", F.avg("quantity_bucket").over(w10))
)

result = (
    scored
    .groupBy(
        "seq_no",
        F.when(F.col("first_known_quantity_bucket") >= 9, "HIGH").otherwise("LOW").alias("first_known_quantity_bucket_tier"),
    )
    .agg(
        F.percentile_approx("order_id_avg_unit_price", 0.9).alias("order_id_avg_unit_price_p90"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("distinct_product_ids").desc_nulls_last())
)
