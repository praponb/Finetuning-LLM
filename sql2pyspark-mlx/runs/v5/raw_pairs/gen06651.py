from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first(), "id")

deduped = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("rating") >= 3.7, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .filter(F.col("rating_p90") > 0)
    .orderBy(F.col("rating_p90").desc())
)
