from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("revenue", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-04-06")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.ceil("revenue").alias("revenue_whole"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "units", "revenue")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .withColumn("first_known_revenue", F.first("revenue", ignorenulls=True).over(w3))
    .withColumn("revenue_bucket", F.ntile(3).over(w4))
)

with_dims = (
    metrics
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.revenue",
        "s.rank_in_group",
        "s.revenue_bucket",
        F.col("d.opened_date"),
        F.col("d.store_name"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "revenue",
        "revenue_bucket",
        "opened_date",
        "store_name",
        F.when(F.col("store_name").rlike("^(Downtown Express|Mall Central)$"), "EXTENDED").when(F.col("store_name").rlike("^(Siam Flagship|Airport Kiosk)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("store_name_zone"),
    )
    .orderBy(F.col("revenue_bucket").desc_nulls_first())
)
