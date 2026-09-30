from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
    )
    .filter(F.col("bangkok_rating") > 0)
)
