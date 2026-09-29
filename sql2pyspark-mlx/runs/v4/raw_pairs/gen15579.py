from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")

with_windows = (
    df_inventory
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("product_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w2))
    .withColumn("quantity_bucket", F.ntile(5).over(w3))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("product_id_stddev_samp_quantity") >= 261, "HIGH").otherwise("LOW").alias("product_id_stddev_samp_quantity_tier"),
        F.date_trunc("week", "last_restock").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("product_id_stddev_samp_quantity", [0.25, 0.5, 0.75]).alias("product_id_stddev_samp_quantity_quartiles"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
