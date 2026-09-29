from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("rating", "id")
w3 = Window.partitionBy("city", "category").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_product_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.stock_qty").alias("product_stock_qty"))
)

metrics = (
    with_last
    .select(
        "id",
        "supplier_name",
        "city",
        "category",
        "product_stock_qty",
        F.lag("rating").over(w2).alias("prev_rating"),
        F.lead("rating").over(w2).alias("next_rating"),
        F.lead("product_stock_qty").over(w2).alias("next_product_stock_qty"),
        F.lag("product_stock_qty").over(w2).alias("prev_product_stock_qty"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("rating").over(w3).alias("rating_avg_5"),
    )
)

with_windows = (
    metrics
    .filter(F.col("seq_no") < 3)
    .select(
        "id",
        "supplier_name",
        "product_stock_qty",
        "prev_rating",
        "prev_product_stock_qty",
        "rating_avg_5",
        F.first("prev_rating", ignorenulls=True).over(w4).alias("first_known_prev_rating"),
        F.sum("prev_rating").over(w5).alias("cumulative_prev_rating"),
    )
)

base = (
    with_windows
    .filter(F.upper(F.trim("supplier_name")) == "SIAM PARTS CO")
    .select(
        "id",
        "supplier_name",
        "product_stock_qty",
        "prev_rating",
        "prev_product_stock_qty",
        "rating_avg_5",
        "first_known_prev_rating",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.category").isin(["Books", "Fashion"])), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"), F.col("c.launch_date"))
)

result = (
    with_latest
    .select(
        "id",
        "product_stock_qty",
        "prev_rating",
        "prev_product_stock_qty",
        "first_known_prev_rating",
        "product_brand",
        F.when(F.col("product_brand").rlike("^(Umbrella|Globex)$"), "OTHER_ZONE").when(F.col("product_brand").rlike("^(Acme|Initech)$"), "EXTENDED").otherwise("GROUP_B").alias("product_brand_zone"),
    )
    .orderBy("prev_product_stock_qty")
)
