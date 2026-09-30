from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

latest = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "quantity")
)

result = (
    latest
    .groupBy(
        F.when(F.col("quantity") >= 204, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .filter(F.col("quantity_stddev") >= 0)
    .orderBy(F.col("min_quantity").desc())
)
