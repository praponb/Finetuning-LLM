from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("rating").desc_nulls_last())
w_running = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

joined = (
    latest_match
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.product_name",
        "e.stock_qty",
        "e.tags",
        "e.supplier_id",
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "product_name",
        "stock_qty",
        "supplier_id",
        "rating",
        "supplier_name",
        "country",
        "rn",
        F.lag("stock_qty").over(w_running).alias("prev_stock_qty"),
        F.lead("stock_qty").over(w_running).alias("next_stock_qty"),
        F.lag("rating").over(w_running).alias("prev_rating"),
        F.lead("rating").over(w_running).alias("next_rating"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

result = (
    with_windows
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "supplier_id",
        "rating",
        "supplier_name",
        "prev_stock_qty",
        "next_stock_qty",
        "prev_rating",
        "next_rating",
        F.when(F.col("product_name").rlike("^(Running Shoes|Yoga Mat)$"), "GROUP_B").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "OTHER_ZONE").otherwise("CORE").alias("product_name_zone"),
        F.col("avg_product_cost"),
    )
)
