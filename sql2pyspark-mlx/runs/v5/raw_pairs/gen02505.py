from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("last4_max_rating", "id")
w3 = Window.partitionBy("country").orderBy("last4_count_rating", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, -1)

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.when(F.col("country").isin(["US", "SG"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

enriched = (
    valid
    .withColumn("last4_sum_rating", F.sum("rating").over(w_hist))
    .withColumn("last4_count_rating", F.count("rating").over(w_hist))
    .withColumn("last4_max_rating", F.max("rating").over(w_hist))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Sports", "Home", "Books"])), "left_anti")
)

with_windows = (
    not_flagged
    .withColumn("last4_max_rating_bucket", F.ntile(10).over(w2))
    .withColumn("last4_max_rating_avg_2_prior", F.avg("last4_max_rating").over(w3))
    .withColumn("last4_max_rating_avg_5_prior", F.avg("last4_max_rating").over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "country",
        "rating",
        "country_group",
        "city_clean",
        "last4_sum_rating",
        "last4_count_rating",
        "last4_max_rating_bucket",
        "last4_max_rating_avg_2_prior",
        F.when(F.col("country").rlike("^(JP|TH)$"), "EXTENDED").when(F.col("country").rlike("^(US|SG)$"), "GROUP_A").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("last4_sum_rating").desc())
)
