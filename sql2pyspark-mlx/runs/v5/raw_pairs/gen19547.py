from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("store_name_length").desc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first(), "id")
w3 = Window.orderBy(F.col("store_name_length").desc_nulls_last())
w_recent = Window.orderBy("sqft_rounded", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy("store_name_length_num", "id")
w6 = Window.orderBy(F.col("sqft_rounded").asc_nulls_last())
w_recent_2 = Window.orderBy("sqft_rounded", "id")
w8 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("next_store_name_length").desc_nulls_last())

filtered = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-03-04").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.length("store_name").alias("store_name_length"),
        F.round("sqft", 0).alias("sqft_rounded"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

cleaned = (
    eligible
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-07"))) & F.col("store_name").like("A%"))
    .withColumn("store_name_length_num", F.col("store_name_length").cast("double"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "opened_date",
        "store_name_length",
        "sqft_rounded",
        "region_clean",
        "store_name_length_num",
    )
)

enriched = (
    ranked
    .withColumn("store_name_length_cume_dist", F.cume_dist().over(w3))
    .withColumn("store_name_length_num_avg_5", F.avg("store_name_length_num").over(w_recent))
    .withColumn("next_store_name_length", F.lead("store_name_length", 1).over(w_running))
    .withColumn("sqft_rounded_percent_rank", F.percent_rank().over(w6))
    .withColumn("seq_no", F.row_number().over(w_recent_2))
)

result = (
    enriched
    .select(
        "id",
        "sqft_rounded",
        "store_name_length_num",
        "store_name_length_num_avg_5",
        "next_store_name_length",
        "sqft_rounded_percent_rank",
        "seq_no",
        F.avg("sqft_rounded").over(w8).alias("running_avg_sqft_rounded"),
        F.min("sqft_rounded").over(w8).alias("running_min_sqft_rounded"),
        F.count("sqft_rounded").over(w8).alias("running_count_sqft_rounded"),
        F.max("sqft_rounded").over(w8).alias("running_max_sqft_rounded"),
        F.rank().over(w9).alias("next_store_name_length_rank"),
    )
)
