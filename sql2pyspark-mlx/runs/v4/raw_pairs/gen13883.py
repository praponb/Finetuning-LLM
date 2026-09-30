from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("week_start").orderBy(F.col("sqft_bit_and").desc())

latest = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("week", "opened_date").alias("week_start"),
        F.when(F.col("sqft") >= 12680, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles_2"),
    )
    .withColumn("sqft_bit_and_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("sqft_bit_and").asc_nulls_last())
)
