from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.orderBy("rating", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy("country").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH", "UK"]))
    .withColumn("city_prefix", F.substring("city", 1, 3))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.launch_date"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "sale")), "left_anti")
)

scored = (
    eligible
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "city",
        "resolved_city",
        F.avg("price").over(w2).alias("price_avg_3_prior"),
        F.ntile(3).over(w3).alias("rating_bucket"),
        F.sum("rating").over(w4).alias("running_rating"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.month(F.col("x.launch_date")).isin([3, 10, 12])), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "city",
        "resolved_city",
        "price_avg_3_prior",
        "rating_bucket",
        "running_rating",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
