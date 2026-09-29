from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("launch_date", "id")
w_part = Window.orderBy("rating", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first())
w6 = Window.partitionBy("city", "country")
w7 = Window.partitionBy("country")
w8 = Window.orderBy("rating_percent_rank", "id")

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

enriched = (
    latest_match
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w2))
    .withColumn("prev_rating", F.lag("rating", 1).over(w_hist))
    .withColumn("rating_avg_6_prior", F.avg("rating").over(w_part))
    .withColumn("rating_percent_rank", F.percent_rank().over(w5))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w6)), 4))
)

eligible = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= F.col("s.launch_date")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price").between(340, 920)), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= F.col("s.launch_date")), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.round((F.col("prev_rating") / F.sum("prev_rating").over(w7)), 4).alias("prev_rating_share"),
        F.lag("rating", 1).over(w8).alias("prev_rating_2"),
    )
)

remaining_keys = (
    metrics
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") > 3.5)
        .select("city", "country")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "launch_date",
        "prev_rating",
        "rating_avg_6_prior",
        "rating_percent_rank",
        "prev_rating_share",
    )
    .orderBy(F.col("prev_rating_share").desc_nulls_last())
)
