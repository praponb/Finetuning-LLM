from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("rating").desc(), "id")
w_running = Window.orderBy(F.col("rating_int").asc_nulls_last())
w4 = Window.orderBy(F.col("rating").asc_nulls_last())
w5 = Window.orderBy(F.col("rating_int").asc_nulls_first())

base = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Bangkok", "Singapore"]))
    .select(
        "id",
        "rating",
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
        F.col("rating").cast("int").alias("rating_int"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") > 670), "left_anti")
)

scored = (
    eligible
    .withColumn("running_rating_int", F.sum("rating_int").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("rating_int_percent_rank", F.percent_rank().over(w_running))
)

remaining_keys = (
    scored
    .select("id")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") >= 2.0)
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty").between(250, 272)), "left_semi")
)

with_windows = (
    kept
    .select(
        "*",
        F.cume_dist().over(w4).alias("rating_cume_dist"),
        F.percent_rank().over(w5).alias("rating_int_percent_rank_2"),
    )
)

base_suppliers = (
    with_windows
    .filter(F.col("rating_int_percent_rank") > 0.63)
    .select(
        "id",
        "rating",
        "rating_int",
        "running_rating_int",
        "event_seq",
        "rating_cume_dist",
        "rating_int_percent_rank_2",
        (F.col("running_rating_int") + F.col("rating_int")).alias("running_rating_int_plus_rating_int"),
    )
)

result = (
    base_suppliers
    .select(
        "id",
        "rating",
        "rating_int",
        "running_rating_int",
        "rating_int_percent_rank_2",
        "running_rating_int_plus_rating_int",
    )
    .orderBy(F.col("running_rating_int").asc_nulls_first())
)
