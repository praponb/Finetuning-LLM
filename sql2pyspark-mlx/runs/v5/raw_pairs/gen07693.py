from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo")
w2 = Window.partitionBy("customer_id").orderBy(F.col("is_welcome5").desc_nulls_first())
w3 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

lines_carts = (
    lines
    .select(
        "customer_id",
        "created_at",
        "promo",
        F.explode("coupon_codes").alias("value"),
    )
)

cleaned = (
    lines_carts
    .filter((F.col("promo").eqNullSafe(F.lit("SPRING10"))) & (F.col("created_at") < "2024-02-01"))
    .select(
        "*",
        F.when(F.col("value") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("customer_id_stddev_samp_is_welcome5", F.stddev_samp("is_welcome5").over(w))
    .withColumn("is_welcome5_rank", F.rank().over(w2))
)

cleaned_carts = (
    enriched
    .filter(F.col("value").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "promo",
        "value",
        "is_welcome5",
        "customer_id_stddev_samp_is_welcome5",
        F.lower("value").alias("value_lower"),
        F.coalesce("value", F.lit("NONE")).alias("value_clean"),
    )
)

enriched_carts = (
    cleaned_carts
    .withColumn("customer_id_stddev_samp_is_welcome5_count_2h", F.count("customer_id_stddev_samp_is_welcome5").over(w3))
)

daily = (
    enriched_carts
    .groupBy(
        F.when(F.col("customer_id_stddev_samp_is_welcome5") >= 63, "LARGE").otherwise("SMALL").alias("customer_id_stddev_samp_is_welcome5_tier"),
        F.date_trunc("hour", "created_at").alias("hour_window"),
    )
    .agg(
        F.avg(F.when(F.col("value") == "WELCOME5", F.col("is_welcome5"))).alias("welcome5_avg_is_welcome5"),
        F.size(F.collect_set("value")).alias("num_values"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
    )
    .filter(F.col("welcome5_events") > 1)
)

result = (
    daily
    .select(
        "customer_id_stddev_samp_is_welcome5_tier",
        "hour_window",
        "welcome5_avg_is_welcome5",
    )
    .orderBy(F.col("hour_window").desc(), F.col("welcome5_avg_is_welcome5").desc())
)
