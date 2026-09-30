from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft").asc_nulls_last())
w5 = Window.orderBy("sqft_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w7 = Window.partitionBy("city", "region")
w8 = Window.partitionBy("city", "region").orderBy(F.col("next_sqft").asc_nulls_first())
w9 = Window.orderBy("seq_no")

with_windows = (
    df_stores
    .select(
        "*",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("sqft").over(w3).alias("sqft_avg_3_prior"),
        F.dense_rank().over(w4).alias("sqft_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-17"))) & (F.col("opened_date") > "2023-08-01"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "next_sqft",
        "seq_no",
        "sqft_avg_3_prior",
        "sqft_dense_rank",
        F.date_sub("opened_date", 85).alias("opened_date_minus_85d"),
    )
)

scored = (
    cleaned
    .withColumn("cumulative_sqft", F.sum("sqft").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("city_stddev_samp_next_sqft", F.stddev_samp("next_sqft").over(w7))
    .withColumn("next_sqft_cume_dist", F.cume_dist().over(w8))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w9))
)

result = (
    scored
    .groupBy("region")
    .pivot("city", ["Berlin", "Tokyo"])
    .agg(F.avg("seq_no"))
)
