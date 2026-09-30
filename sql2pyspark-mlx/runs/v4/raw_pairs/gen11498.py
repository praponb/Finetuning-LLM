from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").asc_nulls_last(), "id")
w2 = Window.orderBy(F.col("cost").desc_nulls_first())
w3 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty_whole").desc(), "id")
w5 = Window.partitionBy("supplier_id").orderBy("price", "id")
w6 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")
w7 = Window.partitionBy("supplier_id").orderBy(F.col("rank_in_group").desc(), "id")

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-25").cast("date"))
    .withColumn("product_name_upper", F.upper("product_name"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    ranked
    .filter(F.col("rank_in_group") <= 1)
    .select(
        "id",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        "product_name_upper",
        "brand_filled",
        "rank_in_group",
        F.floor("stock_qty").alias("stock_qty_whole"),
        F.when(F.col("brand").isin(["Acme", "Umbrella"]), "Tier 1").otherwise("Other").alias("brand_group"),
    )
)

enriched = (
    cleaned
    .withColumn("cost_rank", F.rank().over(w2))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("cost_bucket", F.ntile(3).over(w6))
)

ranked_products = (
    enriched
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked_products
    .select(
        "id",
        "price",
        "supplier_id",
        "brand_filled",
        "stock_qty_whole",
        "brand_group",
        "cost_rank",
        "last_known_cost",
        "seq_no_2",
    )
    .orderBy(F.col("cost_rank").asc_nulls_last())
)
