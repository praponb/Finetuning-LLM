from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id")
w2 = Window.orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_last())
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue_bucket").asc_nulls_last())
w5 = Window.orderBy("revenue_int")
w6 = Window.partitionBy("store_id")

cleaned = (
    df_store_sales
    .filter(F.month("sale_date").isin([5, 7, 9]))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

scored = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(3).over(w2).alias("revenue_bucket"),
        F.rank().over(w3).alias("units_rank"),
    )
)

matched_keys = (
    scored
    .select("store_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("sale_date") < "2025-04-01")
        .select("store_id", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("revenue_bucket").between(4, 6))
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)

scored_store_sales = (
    base
    .filter(F.col("revenue") >= 1600)
    .select(
        "store_id",
        "revenue_bucket",
        "units_rank",
        "revenue_int",
        F.percent_rank().over(w4).alias("revenue_bucket_percent_rank"),
        F.dense_rank().over(w5).alias("revenue_int_dense_rank"),
    )
)

result = (
    scored_store_sales
    .select(
        "store_id",
        "revenue_bucket",
        "units_rank",
        "revenue_int",
        "revenue_int_dense_rank",
        F.round((F.col("revenue_bucket") / F.sum("revenue_bucket").over(w6)), 4).alias("revenue_bucket_share"),
    )
)
