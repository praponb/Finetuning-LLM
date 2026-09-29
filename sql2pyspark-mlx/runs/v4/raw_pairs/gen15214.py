from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")
w_hist = Window.orderBy(F.col("city_avg_rating").asc_nulls_first())
w4 = Window.orderBy(F.col("city_max_rating").desc_nulls_first())
w5 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first())
w6 = Window.partitionBy("country").orderBy(F.col("city_avg_rating").asc_nulls_first())
w7 = Window.orderBy(F.col("rating").desc())

kept_keys = (
    df_suppliers
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") >= 2.4)
        .select("country")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.min("rating").over(w).alias("country_min_rating"),
        F.max("rating").over(w2).alias("city_max_rating"),
        F.avg("rating").over(w2).alias("city_avg_rating"),
    )
)

metrics = (
    scored
    .withColumn("city_avg_rating_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("city_max_rating_percent_rank", F.percent_rank().over(w4))
    .withColumn("rating_dense_rank", F.dense_rank().over(w5))
    .withColumn("city_max_city_max_rating", F.max("city_max_rating").over(w2))
    .withColumn("city_avg_city_avg_rating", F.avg("city_avg_rating").over(w2))
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") < 640), "left_anti")
)

result = (
    eligible
    .withColumn("city_avg_rating_rank", F.rank().over(w6))
    .withColumn("city_avg_city_avg_rating_share", F.round((F.col("city_avg_city_avg_rating") / F.sum("city_avg_city_avg_rating").over(w)), 4))
    .withColumn("rating_dense_rank_2", F.dense_rank().over(w7))
)
