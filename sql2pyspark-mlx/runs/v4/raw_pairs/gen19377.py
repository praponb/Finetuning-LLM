from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("category").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("category").orderBy("sale_date_plus_months", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("category").orderBy(F.col("next_price").desc_nulls_first(), "id")
w6 = Window.partitionBy("brand").orderBy("brand_sum_units", "id")

valid = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 1800))
    .select("*", F.add_months("sale_date", 3).alias("sale_date_plus_months"))
)

joined = (
    valid
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.brand"), F.col("b.category"), F.col("b.price"))
)

with_windows = (
    joined
    .select(
        "*",
        F.sum("units").over(w).alias("brand_sum_units"),
        F.lead("price", 1).over(w2).alias("next_price"),
        F.first("category", ignorenulls=True).over(w3).alias("first_known_category"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("price").over(w_part).alias("price_avg_6"),
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "sale_date_plus_months").alias("week_start"))
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.percentile_approx("next_price", [0.25, 0.5, 0.75]).alias("next_price_quartiles"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("fashion_count").desc_nulls_last())
)
