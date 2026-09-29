from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w_part = Window.orderBy("age", "id")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country").orderBy(F.col("age_bucket").desc(), "id")

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2024-02-01").cast("date")) & F.col("city").isNotNull() & (F.col("credit_limit") < 4500))
    .select(
        "id",
        "name",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.add_months("signup_date", 12).alias("signup_date_plus_months"),
    )
)

enriched = (
    base
    .withColumn("age_bucket", F.ntile(5).over(w))
    .withColumn("age_bucket_2", F.ntile(4).over(w_part))
    .withColumn("city_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w3))
)

filtered = (
    enriched
    .filter(F.col("city") == "Berlin")
    .withColumn("age_bucket_2_filled", F.coalesce("age_bucket_2", F.lit(0)))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "name",
        "country",
        "is_active",
        "credit_limit",
        "signup_date_plus_months",
        "age_bucket",
        "age_bucket_2",
        "city_stddev_samp_credit_limit",
        F.when(F.col("name").rlike("^(Pim Chaiyaporn|Grace Kim)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("name_zone"),
    )
)
