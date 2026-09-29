from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id", "store_id")
w5 = Window.orderBy("revenue", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("sale_date").asc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter(F.col("revenue") >= 2600)
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

enriched = (
    filtered
    .filter(F.col("revenue") < 1100)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "sale_date_month_end",
        F.ntile(5).over(w).alias("revenue_bucket"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.lag("revenue", 1).over(w3).alias("prev_revenue"),
        F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4).alias("revenue_share"),
        F.ntile(5).over(w5).alias("revenue_bucket_2"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

with_dims = (
    deduped
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.sqft"), F.col("s.region"), F.col("s.opened_date"))
)

result = (
    with_dims
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "sale_date_month_end",
        "corr_revenue_units",
        "revenue_share",
        "revenue_bucket_2",
        "region",
        F.when(F.col("region").rlike("^(West|Central)$"), "OTHER_ZONE").when(F.col("region").rlike("^(North|South)$"), "GROUP_A").otherwise("GROUP_B").alias("region_zone"),
    )
    .orderBy(F.col("sale_date_month_end").desc())
)
