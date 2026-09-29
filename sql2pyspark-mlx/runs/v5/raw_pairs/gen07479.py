from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("units") <= 25)
    .select(
        "id",
        "sale_date",
        "revenue",
        F.round((F.col("revenue") * 1.07), 2).alias("revenue_scaled"),
    )
)

filtered = (
    valid
    .filter(F.col("revenue") >= 3900)
    .select(
        "*",
        F.least("revenue", "revenue_scaled").alias("least_revenue_revenue_scaled"),
        (F.col("revenue_scaled") + F.col("revenue")).alias("revenue_scaled_plus_revenue"),
    )
)

base = (
    filtered
    .filter(F.col("revenue_scaled") <= 49)
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_scaled_plus_revenue",
        F.when(F.col("revenue") >= 4000, "High").when(F.col("revenue") >= 700, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

filtered_store_sales = (
    base
    .filter(F.col("sale_date") >= F.lit("2023-04-11").cast("date"))
    .withColumn("revenue_per_revenue_scaled_plus_revenue", (F.col("revenue") / F.nullif("revenue_scaled_plus_revenue", F.lit(0))))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 6))
)

result = (
    filtered_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_scaled_plus_revenue",
        "revenue_band",
        "revenue_per_revenue_scaled_plus_revenue",
        "sale_date_plus_months",
    )
    .orderBy(F.col("revenue").asc_nulls_last())
)
