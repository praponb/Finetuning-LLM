from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())

base = (
    df_stores
    .filter(F.col("opened_date") > "2025-03-01")
    .select(
        "*",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= "2025-09-01"), "left_anti")
)

ranked = (
    kept
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept_stores = (
    ranked
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    kept_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "opened_date_label",
        "region_clean",
        "dedup_rank",
    )
)
