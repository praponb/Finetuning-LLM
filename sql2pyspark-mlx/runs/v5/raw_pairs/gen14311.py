from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("age").desc())
w2 = Window.partitionBy("city").orderBy(F.col("age_percent_rank").desc_nulls_last())

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

joined = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(~F.col("d.is_active"))
    .select("a.id", "a.customer_id", "a.code", F.col("d.city"))
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.age"))
)

enriched = joined_carts.withColumn("age_percent_rank", F.percent_rank().over(w))

base = (
    enriched
    .filter(F.col("city").isNotNull())
    .withColumn("clean_city", F.trim("city"))
)

enriched_carts = (
    base
    .select(
        "customer_id",
        "code",
        "city",
        "age",
        "age_percent_rank",
        "clean_city",
        F.rank().over(w2).alias("age_percent_rank_rank"),
    )
)

result = (
    enriched_carts
    .select(
        "customer_id",
        "code",
        "city",
        "age",
        "age_percent_rank",
        "clean_city",
        "age_percent_rank_rank",
        F.when(F.col("code").rlike("^(WELCOME5|SPRING10)$"), "GROUP_B").when(F.col("code").rlike("^(VIP|BUNDLE)$"), "PRIORITY").otherwise("EXTENDED").alias("code_zone"),
    )
    .orderBy("code")
)
