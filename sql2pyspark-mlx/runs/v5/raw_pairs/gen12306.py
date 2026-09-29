from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("promo", "customer_id")
w3 = Window.orderBy(F.col("product_id").desc_nulls_last())

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.age"), F.col("b.name"))
)

base = (
    joined
    .filter(~F.col("promo").eqNullSafe(F.lit("VIP")))
    .select("*", F.size("coupon_codes").alias("coupon_codes_count"))
)

metrics = (
    base
    .filter(F.col("name").isNotNull())
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w2)), 4))
    .withColumn("product_id_dense_rank", F.dense_rank().over(w3))
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "qty",
        "promo",
        "age",
        "product_id_dense_rank",
        F.when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "PRIORITY").when(F.col("promo").rlike("^(SPRING10|BUNDLE)$"), "GROUP_B").otherwise("CORE").alias("promo_zone"),
    )
)
