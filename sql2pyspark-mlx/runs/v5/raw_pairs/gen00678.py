from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft").desc(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-01").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").isin(["Central", "West"]), "Group A").otherwise("Other").alias("region_group"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)

valid = (
    kept
    .filter(F.col("region") == "Central")
    .select("*", F.initcap(F.trim("city")).alias("city_title"))
)

metrics = (
    valid
    .filter(F.col("sqft") <= 4686)
    .select(
        "*",
        F.first("sqft_dense_rank", ignorenulls=True).over(w3).alias("first_known_sqft_dense_rank"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "region_group",
        "first_known_region",
        "first_known_sqft_dense_rank",
        "event_seq",
    )
    .orderBy(F.col("region").desc_nulls_last())
)
