from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_first())
w2 = Window.partitionBy("category", "city").orderBy(F.col("row_num").desc(), "id")
w3 = Window.partitionBy("category").orderBy("price", "id")
w4 = Window.partitionBy("supplier_id").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("cost").desc_nulls_last())
w6 = Window.partitionBy("city").orderBy("rating", "id")

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("category") == "Home"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.rating"), F.col("p2.city"), F.col("p2.country"))
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select(
        "*",
        F.ntile(3).over(w3).alias("price_bucket"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country"),
        F.cume_dist().over(w5).alias("cost_cume_dist"),
        F.row_number().over(w6).alias("row_seq"),
    )
)
