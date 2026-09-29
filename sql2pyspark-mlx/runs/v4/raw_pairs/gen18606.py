from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .filter(F.col("quantity_p95") < 0)
)
