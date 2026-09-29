from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.orderBy(F.col("rating").desc_nulls_first())
w5 = Window.orderBy("rating_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("rating_bucket").desc_nulls_last())
w7 = Window.orderBy(F.col("rating_bucket").desc(), "id")
w8 = Window.orderBy("prev_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("rating_cume_dist", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("rating") >= 3.5)
    .withColumn("city_lower", F.lower("city"))
)

with_windows = (
    base
    .select(
        "id",
        "rating",
        "city_lower",
        F.lag("rating", 1).over(w_grp).alias("prev_rating"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
)

enriched = (
    with_windows
    .filter(F.col("next_rating") <= 3.2)
    .select(
        "id",
        "city_lower",
        "prev_rating",
        F.ntile(4).over(w3).alias("rating_bucket"),
        F.cume_dist().over(w4).alias("rating_cume_dist"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "eco")), "left_anti")
)

enriched_suppliers = (
    kept
    .select(
        "id",
        "city_lower",
        "prev_rating",
        "rating_bucket",
        F.sum("rating_cume_dist").over(w5).alias("running_rating_cume_dist"),
        F.cume_dist().over(w6).alias("rating_bucket_cume_dist"),
        F.ntile(10).over(w7).alias("rating_bucket_bucket"),
        F.sum("prev_rating").over(w8).alias("running_prev_rating"),
        F.avg("prev_rating").over(w9).alias("prev_rating_avg_3"),
    )
)

result = (
    enriched_suppliers
    .select(
        "id",
        "city_lower",
        "prev_rating",
        "rating_bucket",
        "rating_bucket_cume_dist",
        "rating_bucket_bucket",
        "prev_rating_avg_3",
    )
    .orderBy(F.col("id").desc(), "prev_rating")
)
