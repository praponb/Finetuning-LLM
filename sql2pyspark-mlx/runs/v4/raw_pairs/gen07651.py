from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

ranked = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("revenue") >= 3800, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        "store_id",
        F.when(F.col("units") >= 39, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.max("revenue").alias("max_revenue"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
    )
)
