from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, -1)
w_hist = Window.partitionBy("city")
w4 = Window.partitionBy("city", "country").orderBy(F.col("rating_avg_3_prior").desc(), "id")
w5 = Window.partitionBy("country")

base = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select("*", F.trim("supplier_name").alias("clean_supplier_name"))
)

metrics = (
    base
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("rating_avg_3_prior", F.avg("rating").over(w2))
    .withColumn("city_max_rating", F.max("rating").over(w_hist))
)

enriched = (
    metrics
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "clean_supplier_name",
        "event_seq",
        "rating_avg_3_prior",
        F.ntile(4).over(w4).alias("rating_avg_3_prior_bucket"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4).alias("event_seq_share"),
    )
)

valid = (
    enriched
    .filter(F.col("rating_avg_3_prior_bucket") != 3)
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "event_seq",
        "rating_avg_3_prior_bucket",
        "event_seq_share",
        F.ceil("event_seq_share").alias("event_seq_share_whole"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") < 930), "left_semi")
)

result = (
    not_flagged
    .cube("country")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating_avg_3_prior_bucket")).otherwise(0)).alias("jp_rating_avg_3_prior_bucket"),
        F.min("event_seq").alias("min_event_seq"),
        F.avg("rating").alias("avg_rating"),
    )
)
