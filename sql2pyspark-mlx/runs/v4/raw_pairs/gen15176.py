from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy("latest_rank", "id")
w_running = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

latest = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

latest_store_sales = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    latest_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.brand"), F.col("p.launch_date"))
)

scored = (
    with_dims
    .select(
        "*",
        F.lead("revenue", 1).over(w4).alias("next_revenue"),
        F.rank().over(w_running).alias("revenue_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "brand",
        "launch_date",
        "revenue_rank",
        F.when(F.col("brand").rlike("^(Umbrella|Acme)$"), "CORE").when(F.col("brand").rlike("^(Globex|Initech)$"), "GROUP_B").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy(F.col("launch_date").desc_nulls_last())
)
