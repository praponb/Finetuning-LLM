from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

deduped = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["rating_tier", "country"], ["rating_tier"], []],
        "rating_tier",
        "country",
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("rating_per_tokyo"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
    .filter(F.col("rating_per_tokyo") < 0)
    .orderBy(F.col("total_rating").desc_nulls_last())
)
