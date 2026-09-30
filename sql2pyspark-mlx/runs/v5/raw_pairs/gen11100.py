from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("revenue") >= 1700)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

cleaned = (
    valid
    .filter(F.col("sale_date") >= F.lit("2023-03-23").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
    )
)

remaining_keys = (
    cleaned
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 6]))
        .select("product_id", "store_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("product_id")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date").between("2025-01-01", "2025-02-01"))
        .select("product_id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units").between(25, 37))
        .select("product_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id") == 3)
        .select("store_id", "id")
    )
)

reconciled_store_sales = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled_store_sales
    .select("id", "store_id", "product_id", "sale_date")
    .orderBy("store_id", F.col("id").desc())
)
