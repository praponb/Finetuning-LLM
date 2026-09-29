from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.upper("country").alias("country_upper"),
    )
)

valid = (
    filtered
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
)

filtered_suppliers = (
    valid
    .filter(F.col("supplier_name").like("T%"))
    .select(
        "id",
        "supplier_name",
        "city",
        F.concat("city", F.lit(" - "), "country").alias("city_country"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

base = (
    filtered_suppliers
    .filter(F.upper("supplier_name").like("%IAM%"))
    .select(
        "id",
        "supplier_name",
        "city",
        "resolved_city",
        F.substring("supplier_name", 1, 2).alias("supplier_name_prefix"),
        F.lower("supplier_name").alias("supplier_name_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "city",
        "resolved_city",
        "supplier_name_prefix",
        "supplier_name_lower",
        "city_filled",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
