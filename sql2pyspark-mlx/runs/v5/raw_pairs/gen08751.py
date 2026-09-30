from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("units_num").desc(), "id")
w2 = Window.orderBy("greatest_revenue_whole_rn", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("greatest_revenue_whole_rn").desc_nulls_last())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-07-17").cast("date")) & (F.col("sale_date") > "2024-09-13"))
    .withColumn("units_num", F.col("units").cast("double"))
    .withColumn("revenue_whole", F.ceil("revenue"))
)

joined = (
    base
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.units_num",
        "a.revenue_whole",
        F.col("p.product_name"),
        F.col("p.category"),
    )
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base_store_sales = (
    ranked
    .filter(F.col("rn") != 1)
    .select(
        "id",
        "product_id",
        "revenue_whole",
        "product_name",
        "category",
        F.greatest("revenue_whole", "rn").alias("greatest_revenue_whole_rn"),
        F.substring("product_name", 1, 4).alias("product_name_prefix"),
    )
)

metrics = (
    base_store_sales
    .withColumn("greatest_revenue_whole_rn_avg_2_prior", F.avg("greatest_revenue_whole_rn").over(w2))
    .withColumn("greatest_revenue_whole_rn_percent_rank", F.percent_rank().over(w3))
)

result = (
    metrics
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "product_name_prefix",
        "greatest_revenue_whole_rn_avg_2_prior",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(
        F.col("greatest_revenue_whole_rn_avg_2_prior").desc_nulls_last(),
        "avg_inventory_quantity",
    )
)
