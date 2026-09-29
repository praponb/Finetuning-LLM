from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("created_at_hour").asc_nulls_first())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

cleaned = (
    lines
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & F.col("customer_id").isNotNull())
    .withColumn("clean_element", F.trim("element"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.email"), F.col("d.name"), F.col("d.credit_limit"))
)

cleaned_carts = (
    with_dims
    .filter(F.col("email").isNotNull() & F.col("element").isNotNull())
    .select(
        "id",
        "created_at",
        "element",
        "clean_element",
        "email",
        "name",
        "credit_limit",
        F.when(F.col("credit_limit") >= 14300, "High").when(F.col("credit_limit") >= 8000, "Medium").otherwise("Low").alias("credit_limit_tier"),
        F.coalesce("element", F.lit("UNKNOWN")).alias("resolved_element"),
    )
)

filtered = (
    cleaned_carts
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "element",
        "clean_element",
        "email",
        "name",
        "credit_limit",
        "credit_limit_tier",
        "resolved_element",
        F.upper("name").alias("name_upper"),
        F.hour("created_at").alias("created_at_hour"),
    )
)

cleaned_carts_2 = (
    filtered
    .filter(F.col("element") == "VIP")
    .select(
        "id",
        "element",
        "clean_element",
        "name",
        "credit_limit",
        "credit_limit_tier",
        "resolved_element",
        "created_at_hour",
        F.round((F.col("created_at_hour") * 1.1), 2).alias("adjusted_created_at_hour"),
        F.coalesce("element", F.lit("NONE")).alias("element_filled"),
    )
)

base = (
    cleaned_carts_2
    .filter(F.col("element").isNotNull())
    .select(
        "id",
        "element",
        "clean_element",
        "resolved_element",
        "created_at_hour",
        "adjusted_created_at_hour",
        "element_filled",
        F.coalesce("created_at_hour", F.lit(0)).alias("created_at_hour_filled"),
        F.coalesce("element", F.lit("NONE")).alias("resolved_element_2"),
    )
)

result = (
    base
    .select(
        "id",
        "clean_element",
        "resolved_element",
        "created_at_hour",
        "element_filled",
        "created_at_hour_filled",
        "resolved_element_2",
        F.rank().over(w).alias("created_at_hour_rank"),
    )
)
