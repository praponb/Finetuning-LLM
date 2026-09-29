from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("price", "id")
w3 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w4 = Window.partitionBy("supplier_id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("supplier_id", "category").orderBy(F.col("event_seq_2").desc(), "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

scored = (
    df_products
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("event_seq_2", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("corr_stock_qty_price", F.corr("stock_qty", "price").over(w4))
)

latest_match = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

latest = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "first_known_cost",
        F.when(F.col("product_name").rlike("^(Desk Lamp|  Water Bottle)$"), "GROUP_B").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "CORE").otherwise("PRIORITY").alias("product_name_zone"),
        F.col("max_product_cost"),
    )
    .orderBy(F.col("max_product_cost").asc_nulls_first())
)
