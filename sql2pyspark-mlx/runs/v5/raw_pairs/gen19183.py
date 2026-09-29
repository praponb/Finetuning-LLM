from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-07-28")))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
    )
)

result = (
    base
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["brand", "category"], ["brand"]],
        F.col("d.brand").alias("brand"),
        F.col("d.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.revenue")).alias("avg_revenue"),
        F.round(F.avg(F.col("src.units")), 2).alias("avg_units"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
