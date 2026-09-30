from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("city_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter(F.col("city").isin(["London", "Singapore", "Bangkok"]) & (F.col("region").eqNullSafe(F.lit("West"))))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.lower("region").alias("region_lower"),
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.status") == "SHIPPED"), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.opened_date")), "left_anti")
)

eligible_stores = (
    kept
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") >= F.col("s.opened_date")), "left_anti")
)

kept_stores = (
    eligible_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_anti")
)

cleaned = (
    kept_stores
    .filter(F.col("city_length").isNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "region_lower",
        "city_length",
        F.when(F.col("city_length") >= 89, "High").when(F.col("city_length") >= 35, "Medium").otherwise("Low").alias("city_length_tier"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("city_length").over(w).alias("running_avg_city_length"),
        F.min("city_length").over(w).alias("running_min_city_length"),
        F.max("city_length").over(w).alias("running_max_city_length"),
        F.sum("city_length").over(w2).alias("running_city_length"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "region_lower",
        "running_avg_city_length",
        "running_min_city_length",
        "running_max_city_length",
        "running_city_length",
    )
    .orderBy("running_avg_city_length")
)
