from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-04-01", "2025-06-24"))
    .withColumn("sale_date_period", F.date_trunc("month", "sale_date"))
)

matched_keys = (
    base
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") != 30)
        .select("id", "product_id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.price"))
)

result = (
    joined
    .withColumn("quarter_start", F.date_trunc("QUARTER", "sale_date"))
    .withColumn("units_tier", F.when(F.col("units") >= 36, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "units_tier", "store_id"], ["quarter_start", "units_tier"], []],
        "quarter_start",
        "units_tier",
        "store_id",
    )
    .agg(
        F.max("sale_date_period").alias("last_seen"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
