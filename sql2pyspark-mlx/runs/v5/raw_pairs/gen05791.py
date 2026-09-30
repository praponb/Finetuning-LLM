from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("least_rating_whole_rating").desc())

cleaned = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("London")))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("country").isin(["SG", "UK"]), "Priority").otherwise("Other").alias("country_group"),
        F.floor("rating").alias("rating_whole"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") <= "2025-06-01"), "left_anti")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isin([12, 19])), "left_anti")
)

base = (
    eligible_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("least_rating_whole_rating", F.least("rating_whole", "rating"))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

latest = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.upper(F.col("x.product_name")).like("%SB-%")), "left_anti")
)

stats = (
    not_flagged
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.5, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.min("least_rating_whole_rating").alias("min_least_rating_whole_rating"),
        F.max(F.when(F.col("city") == "Bangkok", F.col("rating_whole"))).alias("bangkok_max_rating_whole"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
    )
)

result = (
    stats
    .select("city", "rating_tier", "bangkok_events")
    .orderBy(F.col("bangkok_events").desc(), F.col("rating_tier").desc())
)
