from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_p95").desc())

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 222, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("quantity_p95") < 0)
    .withColumn("quantity_p95_dense_rank", F.dense_rank().over(w))
)
