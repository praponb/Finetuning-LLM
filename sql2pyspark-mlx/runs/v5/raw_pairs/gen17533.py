from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_recent = Window.orderBy(F.col("country_length").desc(), "id")
w_ordered = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("country_length", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy(F.col("country_length_avg_5_prior").asc_nulls_first())
w7 = Window.orderBy(F.col("rating").asc_nulls_first())
w8 = Window.orderBy(F.col("country_length_avg_5_prior").desc_nulls_first())

cleaned = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "global supply")
    .select(
        "*",
        F.when(F.col("rating") >= 4.1, "High").when(F.col("rating") >= 2.4, "Medium").otherwise("Low").alias("rating_level"),
        F.length("country").alias("country_length"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < "2024-08-22"), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.last("rating", ignorenulls=True).over(w_ordered).alias("last_known_rating"),
        F.avg("country_length").over(w4).alias("country_length_avg_5_prior"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share_2"),
    )
)

remaining_keys = (
    metrics
    .select("city", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") >= 2.9)
        .select("city", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("country_length_avg_5_prior") > 70)
    .select(
        "*",
        F.stddev_samp("rating_share_2").over(w5).alias("country_stddev_samp_rating_share_2"),
        F.percent_rank().over(w6).alias("country_length_avg_5_prior_percent_rank"),
        F.dense_rank().over(w7).alias("rating_dense_rank"),
        F.rank().over(w8).alias("country_length_avg_5_prior_rank"),
        F.max("rating_share").over(w5).alias("country_max_rating_share"),
    )
)

kept_suppliers = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.product_name").like("Cof%")), "left_semi")
)

kept_suppliers_2 = (
    kept_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.upper(F.trim(F.col("x.product_name"))) == "DESK LAMP"), "left_anti")
)

result = (
    kept_suppliers_2
    .groupBy("country")
    .pivot("city", ["New York", "London", "Chiang Mai"])
    .agg(F.max("country_max_rating_share"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("London").alias("london"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
