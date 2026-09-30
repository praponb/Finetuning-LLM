from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w_grp = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("supplier_id").orderBy("price", "id")
w4 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("price", "id")

base = (
    df_products
    .filter(F.col("stock_qty") >= 390)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_contains("tags", "new").alias("is_new"),
    )
)

result = (
    base
    .filter(F.col("cost").between(370, 620))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("cost_rank", F.rank().over(w_grp))
    .withColumn("price_bucket", F.ntile(4).over(w3))
    .withColumn("first_known_brand", F.first("brand", ignorenulls=True).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)
