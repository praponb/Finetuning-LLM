from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

ranked = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("rating") >= 3.7, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_per_singapore"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
    .filter(F.col("rating_per_singapore") < 0)
    .orderBy(F.col("rating_per_singapore").desc())
)
