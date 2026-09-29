from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_hist = Window.orderBy("product_stock_qty", "id")
w3 = Window.orderBy(F.col("product_stock_qty").desc(), "id")
w4 = Window.orderBy(F.col("product_stock_qty").desc_nulls_last())
w5 = Window.orderBy("product_stock_qty", "id").rowsBetween(-5, -1)

valid = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("Tokyo")))
    .withColumn("supplier_name_country_key", F.concat_ws("-", "supplier_name", "country"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.supplier_name_country_key",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") < 144), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.lead("product_stock_qty", 1).over(w_hist).alias("next_product_stock_qty"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.cume_dist().over(w4).alias("product_stock_qty_cume_dist"),
        F.avg("product_stock_qty").over(w5).alias("product_stock_qty_avg_5_prior"),
    )
)

result = (
    with_windows
    .groupBy(
        "seq_no",
        F.when(F.col("next_product_stock_qty") >= 331, "PREMIUM").otherwise("STANDARD").alias("next_product_stock_qty_tier"),
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.sum("product_stock_qty_avg_5_prior").alias("total_product_stock_qty_avg_5_prior"),
        F.min("product_stock_qty").alias("min_product_stock_qty"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
