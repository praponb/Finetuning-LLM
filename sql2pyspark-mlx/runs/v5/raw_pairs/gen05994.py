from pyspark.sql import functions as F

dim_totals = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .groupingSets(
        [["country", "city", "brand"], ["country", "city"], ["country"]],
        F.col("s.country").alias("country"),
        F.col("s.city").alias("city"),
        F.col("e.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.stock_qty")).alias("min_stock_qty"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    dim_totals
    .select(
        "country",
        "brand",
        "num_rows",
        "grouping_level",
        F.when(F.col("country").rlike("^(SG|UK)$"), "CORE").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc())
)
