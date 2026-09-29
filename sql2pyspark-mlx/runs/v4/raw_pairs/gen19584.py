from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("city", "region")
w4 = Window.partitionBy("city").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("region", "city")
w6 = Window.partitionBy("region").orderBy(F.col("quantity_bucket").desc(), "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2024-02-22"))) & F.col("region").isNotNull() & F.col("city").isin(["Singapore", "Bangkok"]))
    .withColumn("city_length", F.length("city"))
    .withColumn("region_title", F.initcap(F.trim("region")))
)

cleaned = (
    valid
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-08-11")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "region_title",
        F.when(F.col("city").isin(["Tokyo", "Berlin"]), "Priority").otherwise("Other").alias("city_group"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

scored = (
    with_latest
    .withColumn("quantity_bucket", F.ntile(4).over(w2))
    .withColumn("city_min_quantity", F.min("quantity").over(w3))
    .withColumn("quantity_bucket_2", F.ntile(3).over(w4))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date").between("2023-12-10", "2024-01-01")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.avg("quantity_bucket_2").over(w5).alias("region_avg_quantity_bucket_2"),
        F.ntile(4).over(w6).alias("quantity_bucket_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sum("quantity_bucket_2").alias("total_quantity_bucket_2"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
)
