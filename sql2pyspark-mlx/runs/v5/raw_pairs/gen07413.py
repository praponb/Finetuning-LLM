from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("last4_count_city_length").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w6 = Window.partitionBy("city").orderBy(F.col("last4_max_city_length").desc(), "id")
w7 = Window.partitionBy("city").orderBy("last4_min_city_length", "id")
w8 = Window.partitionBy("city").orderBy("last4_max_city_length")

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-20"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

scored = (
    cleaned
    .filter(F.col("city_length") <= 36)
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "city_length",
        "region_clean",
        F.count("city_length").over(w_ordered).alias("last4_count_city_length"),
        F.min("city_length").over(w_ordered).alias("last4_min_city_length"),
        F.max("city_length").over(w_ordered).alias("last4_max_city_length"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)

cleaned_stores = (
    scored
    .filter(F.col("last4_max_city_length") > 62)
    .select(
        "*",
        F.round((F.col("cumulative_sqft") / F.col("last4_count_city_length")), 2).alias("cumulative_sqft_per_last4_count_city_length"),
    )
)

with_last = (
    cleaned_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

with_windows = (
    with_last
    .select(
        "*",
        F.ntile(4).over(w4).alias("last4_count_city_length_bucket"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
        F.lead("store_sale_units", 1).over(w7).alias("next_store_sale_units"),
        F.cume_dist().over(w8).alias("last4_max_city_length_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "city_length",
        "region_clean",
        "last4_max_city_length",
        "cumulative_sqft",
        "cumulative_sqft_per_last4_count_city_length",
        F.when(F.col("city").rlike("^(London|New York)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("last4_max_city_length").desc_nulls_last(), "id")
)
