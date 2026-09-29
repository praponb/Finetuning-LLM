from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy("city")
w3 = Window.orderBy(F.col("city_count_rating").desc(), "id")
w4 = Window.partitionBy("city").orderBy("city_count_rating", "id")
w5 = Window.orderBy("city_count_rating", "id")
w6 = Window.partitionBy("country")
w7 = Window.orderBy("country_avg_rating_share", "id")
w8 = Window.orderBy("rating_share_plus_country_avg_rating_share", "id").rowsBetween(-2, -1)
w9 = Window.orderBy(F.col("seq_no_2").desc_nulls_first())
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

enriched = (
    df_suppliers
    .filter(F.col("rating") >= 2.7)
    .withColumn("rating_percent_rank", F.percent_rank().over(w))
    .withColumn("city_count_rating", F.count("rating").over(w2))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
)

enriched_suppliers = (
    enriched
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating_percent_rank",
        "rating_share",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.lag("rating_percent_rank", 1).over(w5).alias("prev_rating_percent_rank"),
        F.avg("rating_share").over(w6).alias("country_avg_rating_share"),
    )
)

filtered = (
    enriched_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("Berlin"))))
    .select(
        "id",
        "supplier_name",
        "rating_share",
        "seq_no_2",
        "country_avg_rating_share",
        F.when(F.col("seq_no") >= 7, "High").when(F.col("seq_no") >= 3, "Medium").otherwise("Low").alias("seq_no_tier"),
        (F.col("rating_share") + F.col("country_avg_rating_share")).alias("rating_share_plus_country_avg_rating_share"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

enriched_suppliers_2 = (
    filtered
    .withColumn("next_country_avg_rating_share", F.lead("country_avg_rating_share", 1).over(w7))
    .withColumn("seq_no_2_avg_2_prior", F.avg("seq_no_2").over(w8))
    .withColumn("seq_no_2_dense_rank", F.dense_rank().over(w9))
)

latest_match = (
    enriched_suppliers_2
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.seq_no_tier",
        "s.seq_no_2_avg_2_prior",
        "s.seq_no_2_dense_rank",
        F.col("c.category"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "seq_no_tier",
        "seq_no_2_dense_rank",
        "category",
        F.when(F.col("category").rlike("^(Sports|Home)$"), "GROUP_B").when(F.col("category").rlike("^(Electronics|Books)$"), "GROUP_A").otherwise("CORE").alias("category_zone"),
    )
)
