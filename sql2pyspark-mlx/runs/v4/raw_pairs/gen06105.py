from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy("city").orderBy("rating", "id")
w5 = Window.orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("prev_rating", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("city").orderBy(F.col("next_rating").desc(), "id")
w9 = Window.partitionBy("city").orderBy("rating_avg_3", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("product_brand").orderBy(F.col("next_rating").desc(), "id")
w11 = Window.partitionBy("product_brand").orderBy("rank_in_group", "id")
w12 = Window.partitionBy("product_brand").orderBy(F.col("first_known_rating").desc(), "id")
w13 = Window.orderBy("rating_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    latest
    .select(
        "*",
        F.first("rating", ignorenulls=True).over(w2).alias("first_known_rating"),
        F.lag("rating", 1).over(w3).alias("prev_rating"),
        F.lead("rating", 1, 0).over(w4).alias("next_rating"),
    )
)

with_windows_suppliers = (
    with_windows
    .withColumn("rating_avg_3", F.avg("rating").over(w5))
    .withColumn("rating_avg_6", F.avg("rating").over(w6))
)

with_last = (
    with_windows_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.price").alias("product_price"),
        F.col("c.brand").alias("product_brand"),
    )
)

deduped = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.avg("next_rating").over(w9).alias("next_rating_avg_5"),
        F.row_number().over(w10).alias("row_seq"),
        F.lag("rating_avg_3", 1, 0).over(w11).alias("prev_rating_avg_3"),
        F.row_number().over(w12).alias("event_seq"),
        F.sum("next_rating").over(w13).alias("running_next_rating"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (~F.col("c.brand").isin(["Globex", "Umbrella", "Acme"])), "left_anti")
)

grouped = (
    eligible
    .groupBy(
        "country",
        F.when(F.col("rating_avg_6") >= 4, "LARGE").otherwise("SMALL").alias("rating_avg_6_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("first_known_rating", [0.25, 0.5, 0.75]).alias("first_known_rating_quartiles"),
        F.count("*").alias("cnt"),
    )
)

result = grouped.select("country", "rating_avg_6_tier", "first_known_rating_quartiles")
