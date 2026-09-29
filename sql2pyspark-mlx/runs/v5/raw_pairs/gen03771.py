from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("brand").orderBy(F.col("prev_rating").desc_nulls_last(), "id")

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand").isNotNull()), "left_semi")
)

scored = (
    eligible
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.sum("rating").over(w2).alias("country_sum_rating"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

latest = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "country", "rating", "prev_rating")
)

result = (
    latest
    .groupBy("country")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.avg(F.when(F.col("country") == "UK", F.col("prev_rating"))).alias("uk_avg_prev_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("rating_per_th"),
    )
    .orderBy(F.col("rating_per_th").desc())
)
