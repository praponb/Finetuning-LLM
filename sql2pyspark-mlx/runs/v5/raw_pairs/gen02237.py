from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())

latest = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "quantity", "unit_price")
)

result = (
    latest
    .groupBy("product_id")
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("unit_price_stddev") < 1000000)
    .orderBy(F.col("unit_price_stddev").desc_nulls_first())
)
