from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "category").orderBy("units", "id")
w4 = Window.partitionBy("category").orderBy(F.col("units_whole").asc_nulls_last())

deduped = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.product_name"), F.col("b.cost"), F.col("b.category"))
)

valid = (
    enriched
    .filter(F.col("latest_rank") >= 1)
    .withColumn("units_whole", F.floor("units"))
)

with_windows = (
    valid
    .select(
        "*",
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

deduped_store_sales = (
    with_windows
    .withColumn("latest_rank_2", F.rank().over(w4))
    .filter(F.col("latest_rank_2") == 1)
)

filtered = (
    deduped_store_sales
    .filter(F.col("sale_date") >= "2024-08-06")
    .select(
        "id",
        "product_id",
        "latest_rank",
        "product_name",
        "cost",
        "category",
        "units_whole",
        "last_known_cost",
        "seq_no",
        "latest_rank_2",
        F.last_day("sale_date").alias("sale_date_month_end"),
        (F.col("latest_rank_2") * F.col("units_whole")).alias("latest_rank_2_times_units_whole"),
        F.coalesce("category", F.lit("N/A")).alias("category_clean"),
    )
)

valid_store_sales = (
    filtered
    .filter(F.col("latest_rank_2") < 1)
    .select(
        "*",
        F.concat_ws("-", "product_name", "category").alias("product_name_category_key"),
        F.coalesce("category", F.lit("N/A")).alias("category_filled"),
    )
)

result = (
    valid_store_sales
    .select(
        "id",
        "product_id",
        "latest_rank",
        "product_name",
        "units_whole",
        "last_known_cost",
        "product_name_category_key",
    )
)
