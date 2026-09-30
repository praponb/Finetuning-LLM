from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.orderBy("store_sale_revenue", "id")
w3 = Window.orderBy("next_sqft")
w4 = Window.orderBy(F.col("opened_date").desc(), "id")
w5 = Window.orderBy("next_store_sale_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("row_seq_2", "id")
w7 = Window.orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-27").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_store_name_key", F.concat_ws("-", "city", "store_name"))
    .withColumn("city_group", F.when(F.col("city").isin(["Tokyo", "Chiang Mai"]), "Core").otherwise("Other"))
    .withColumn("region_clean", F.coalesce("region", F.lit("DIRECT")))
)

latest_match = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.sqft",
        "s.city_store_name_key",
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

scored = (
    latest_match
    .filter(F.col("store_sale_revenue") <= 1200)
    .withColumn("prev_sqft", F.lag("sqft").over(w2))
    .withColumn("next_sqft", F.lead("sqft").over(w2))
    .withColumn("next_store_sale_revenue", F.lead("store_sale_revenue").over(w2))
    .withColumn("prev_store_sale_revenue", F.lag("store_sale_revenue").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_windows = (
    scored
    .select(
        "id",
        "opened_date",
        "prev_sqft",
        "row_seq",
        F.percent_rank().over(w3).alias("next_sqft_percent_rank"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.sum("next_store_sale_revenue").over(w5).alias("cumulative_next_store_sale_revenue"),
    )
)

with_windows_stores = (
    with_windows
    .select(
        "id",
        "prev_sqft",
        "next_sqft_percent_rank",
        "row_seq_2",
        F.lead("row_seq_2").over(w_grp).alias("next_row_seq_2"),
        F.lead("cumulative_next_store_sale_revenue").over(w_grp).alias("next_cumulative_next_store_sale_revenue"),
        F.row_number().over(w_grp).alias("row_seq_3"),
        F.ntile(5).over(w7).alias("row_seq_bucket"),
    )
)

latest_match_stores = (
    with_windows_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"), F.col("c.units"))
)

result = (
    latest_match_stores
    .groupBy(
        F.when(F.col("row_seq_bucket") >= 8, "HIGH").otherwise("LOW").alias("row_seq_bucket_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum("next_sqft_percent_rank").alias("total_next_sqft_percent_rank"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
    )
    .orderBy(F.col("row_seq_bucket_tier").desc())
)
