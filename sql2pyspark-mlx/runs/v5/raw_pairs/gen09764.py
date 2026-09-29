from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_per_unit")
w2 = Window.partitionBy("product_id").orderBy("revenue_per_unit")
w3 = Window.orderBy(F.col("revenue_per_unit").desc_nulls_last())

valid = (
    df_store_sales
    .filter(F.col("units") >= 30)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.add_months("sale_date", 1).alias("sale_date_plus_months"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    ranked
    .select("product_id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") > 3000)
        .select("product_id", "store_id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w2).alias("revenue_per_unit_dense_rank"),
        F.cume_dist().over(w3).alias("revenue_per_unit_cume_dist"),
    )
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("revenue_per_unit_cume_dist", 0.9).alias("revenue_per_unit_cume_dist_p90"),
        F.min("units").alias("min_units"),
    )
    .orderBy(F.col("min_units").desc_nulls_first())
)
