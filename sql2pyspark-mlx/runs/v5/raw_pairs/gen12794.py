from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy("sqft", "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.substring("region", 1, 4).alias("region_prefix"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

scored = (
    with_latest
    .filter(F.col("sqft") < 12729)
    .select(
        "*",
        F.lag("sqft").over(w2).alias("prev_sqft"),
        F.lead("sqft").over(w2).alias("next_sqft"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept = (
    latest
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.store_sale_sale_date")), "left_anti")
)

result = (
    kept
    .groupBy("region")
    .agg(
        F.percentile_approx("prev_sqft", 0.9).alias("prev_sqft_p90"),
        F.round(F.sum("next_sqft") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("next_sqft_per_central"),
    )
    .orderBy("region")
)
