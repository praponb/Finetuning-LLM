from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("units_percent_rank_per_units").desc(), "id")
w4 = Window.partitionBy("brand").orderBy(F.col("units_per_units_percent_rank").desc_nulls_last(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

cleaned = (
    df_store_sales
    .filter(F.col("units") > 34)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("units_percent_rank"),
        F.ntile(5).over(w2).alias("units_bucket"),
    )
)

valid = (
    scored
    .filter(F.col("units_percent_rank") <= 0.63)
    .select(
        "id",
        "product_id",
        "units",
        "revenue_per_unit",
        "units_percent_rank",
        "units_bucket",
        F.round((F.col("units_percent_rank") / F.col("units")), 2).alias("units_percent_rank_per_units"),
        (F.col("units") / F.nullif("units_percent_rank", F.lit(0))).alias("units_per_units_percent_rank"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    deduped
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.stock_qty"), F.col("p.brand"))
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "units",
        "revenue_per_unit",
        "units_percent_rank",
        "units_bucket",
        "units_per_units_percent_rank",
        "cost",
        "stock_qty",
        F.col("avg_store_sale_units"),
    )
    .orderBy(F.col("cost").desc())
)
