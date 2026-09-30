from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_first(), "id")

latest = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "revenue")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") >= 109)
    .select(
        "e.id",
        "e.product_id",
        "e.rank_in_group",
        F.col("p.category"),
        F.col("p.launch_date"),
    )
)

result = (
    joined
    .select("id", "category")
    .orderBy(F.col("category").desc())
)
