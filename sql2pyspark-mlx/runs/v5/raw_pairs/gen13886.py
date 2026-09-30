from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("region").orderBy("store_sale_sale_date", "id")

valid = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-06-25")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

scored = (
    with_latest
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "store_name_normalized",
        "store_sale_sale_date",
        F.lead("sqft").over(w_running).alias("next_sqft"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") < 162), "left_semi")
)

stats = (
    eligible
    .groupBy(
        F.when(F.col("next_sqft") >= 12866, "PREMIUM").otherwise("STANDARD").alias("next_sqft_tier"),
        F.date_trunc("QUARTER", "store_sale_sale_date").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.round(F.sum("next_sqft") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("next_sqft_per_singapore"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
    )
)

result = (
    stats
    .select("next_sqft_tier", "quarter_start", "central_events")
    .orderBy(F.col("next_sqft_tier").desc_nulls_first())
)
