from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

ranked = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("min_product_cost"),
    )
    .orderBy("supplier_id", "cost")
)
