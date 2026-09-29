from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date_year").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue_rounded").desc_nulls_last())
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_last())
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("product_id").orderBy(F.col("revenue_rank").desc_nulls_first())

valid = (
    df_store_sales
    .filter(F.col("units") < 25)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.year("sale_date").alias("sale_date_year"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

valid_store_sales = (
    deduped
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-10-09"))) & (F.col("units") == 37))
    .withColumn("revenue_rounded", F.round("revenue", 1))
)

deduped_store_sales = (
    valid_store_sales
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    deduped_store_sales
    .select("product_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units").isNull())
        .select("product_id", "id")
    )
)

reconciled = (
    deduped_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.stddev_samp("sale_date_year").over(w3).alias("product_id_stddev_samp_sale_date_year"),
        F.rank().over(w4).alias("revenue_rank"),
        F.rank().over(w5).alias("units_rank"),
        F.min("sale_date_year").over(w6).alias("store_id_min_sale_date_year"),
    )
)

metrics = (
    scored
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w6)), 4))
    .withColumn("revenue_rank_rank", F.rank().over(w7))
)

result = (
    metrics
    .groupBy(
        "product_id",
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("revenue_rank_rank", [0.25, 0.5, 0.75]).alias("revenue_rank_rank_quartiles"),
        F.min("store_id_min_sale_date_year").alias("min_store_id_min_sale_date_year"),
        F.bit_or("units").alias("units_bit_or"),
    )
    .filter(F.col("units_bit_or") < 1)
    .orderBy("year_start")
)
