from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w5 = Window.orderBy("sqft", "id")
w6 = Window.partitionBy("city").orderBy(F.col("next_sqft").asc_nulls_first(), "id")

filtered = (
    df_stores
    .filter(F.col("sqft") > 8430)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

enriched = (
    filtered
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w))
    .withColumn("sqft_bucket", F.ntile(4).over(w2))
    .withColumn("sqft_avg_5_prior_2", F.avg("sqft").over(w3))
    .withColumn("next_sqft", F.lead("sqft", 2).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "store_name_prefix",
        "region_filled",
        "sqft_avg_5_prior",
        "sqft_avg_5_prior_2",
        F.when(F.col("city").rlike("^(Bangkok|New York)$"), "GROUP_B").when(F.col("city").rlike("^(Chiang Mai|Singapore)$"), "OTHER_ZONE").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("city").desc())
)
