from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("is_smb").desc())
w4 = Window.partitionBy("country").orderBy("rn", "id")

valid = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .withColumn("is_smb", F.when(F.col("segment") == "SMB", 1).otherwise(0))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("OTHER")))
)

cleaned = (
    valid
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-07-30")))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "is_smb",
        "segment_clean",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.regexp_replace("name", " ", "-").alias("name_slug"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_customers = (
    latest
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

ranked = (
    latest_customers
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("is_smb") >= 63, "PREMIUM").otherwise("STANDARD").alias("is_smb_tier"),
    )
    .agg(
        F.max(F.when(F.col("segment") == "Consumer", F.col("age"))).alias("consumer_max_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("consumer_max_age") < 0)
    .orderBy(F.col("is_smb_tier").desc_nulls_first())
)
