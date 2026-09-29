from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

ranked = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 330, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.count("*").alias("cnt"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .orderBy("product_id")
)
