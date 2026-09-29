from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .withColumn("email_title", F.initcap(F.trim("email")))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.signup_date")), "left_semi")
)

valid = (
    not_flagged
    .filter((F.col("signup_date") > F.lit("2023-10-31").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "age",
        "email_title",
        "rn",
        F.trim("country").alias("clean_country"),
        F.ceil("age").alias("age_whole"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("rn") > 1)
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "age",
        "email_title",
        "clean_country",
        "age_whole",
        "segment_filled",
        F.concat("segment", F.lit(" "), "country").alias("segment_country"),
        F.floor("rn").alias("rn_whole"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "name",
        "email",
        "age",
        "email_title",
        "clean_country",
        "age_whole",
        "segment_filled",
        "rn_whole",
        F.when(F.col("name").rlike("^(Olivia Garcia|Henry Ford)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "GROUP_A").otherwise("PRIORITY").alias("name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
