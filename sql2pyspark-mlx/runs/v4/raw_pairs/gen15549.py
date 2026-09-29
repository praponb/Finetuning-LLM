from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("region", "product_id")
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_store_sales
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("revenue_avg_2_prior", F.avg("revenue").over(w2))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.store_name"), F.col("p.region"), F.col("p.opened_date"))
)

scored = (
    enriched
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w4))
)

result = (
    scored
    .select("id", "region", "revenue_share", F.col("revenue").alias("REVENUE"))
    .unpivot(["id", "region"], ["revenue_share", "REVENUE"], "metric", "reading")
    .filter(F.col("reading").isNotNull())
)
