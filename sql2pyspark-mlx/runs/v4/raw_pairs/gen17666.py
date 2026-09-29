from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo", "customer_id").orderBy(F.col("product_id").desc_nulls_first())
w2 = Window.partitionBy("customer_id", "promo")

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "promo",
        F.explode("coupon_codes").alias("tag"),
    )
)

with_windows = (
    exploded_carts
    .withColumn("product_id_percent_rank", F.percent_rank().over(w))
    .withColumn("customer_id_avg_product_id", F.avg("product_id").over(w2))
)

joined = (
    with_windows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.age"), F.col("p.city"), F.col("p.segment"))
)

base = (
    joined
    .filter(F.col("segment").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "promo",
        "tag",
        "customer_id_avg_product_id",
        "city",
        "segment",
        F.floor("customer_id_avg_product_id").alias("customer_id_avg_product_id_whole"),
        F.when(F.col("promo").isin(["VIP", "SPRING10"]), "Group A").otherwise("Other").alias("promo_group"),
    )
)

result = (
    base
    .groupBy("city")
    .pivot("promo", ["BUNDLE", "SPRING10"])
    .agg(
        F.avg("customer_id_avg_product_id").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "city",
        F.col("BUNDLE_amount").alias("bundle_amount"),
        F.col("BUNDLE_cnt").alias("bundle_cnt"),
        F.col("SPRING10_amount").alias("spring10_amount"),
        F.col("SPRING10_cnt").alias("spring10_cnt"),
    )
)
