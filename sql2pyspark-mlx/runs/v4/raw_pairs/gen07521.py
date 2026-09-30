from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "country").orderBy("rating", "id")
w6 = Window.partitionBy("city", "country")
w7 = Window.partitionBy("city").orderBy("rating_avg_6", "id")
w8 = Window.partitionBy("country").orderBy(F.col("running_rating").desc(), "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w10 = Window.partitionBy("city").orderBy(F.col("running_rating").asc_nulls_last())

enriched = (
    df_suppliers
    .select(
        "*",
        F.sum("rating").over(w).alias("running_rating"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("rating").over(w_hist).alias("rating_avg_6"),
        F.last("city", ignorenulls=True).over(w4).alias("last_known_city"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w5).alias("row_seq_2"),
        F.max("row_seq").over(w6).alias("city_max_row_seq"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w9))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.brand"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= F.col("s.product_launch_date")), "left_anti")
)

deduped_suppliers = (
    not_flagged
    .withColumn("row_num", F.rank().over(w10))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

wide = (
    deduped_suppliers
    .groupBy("brand")
    .pivot("country", ["US", "TH", "UK"])
    .agg(F.sum("rating").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)

result = (
    wide
    .select(
        "brand",
        "TH_amount",
        "TH_item_count",
        "UK_amount",
        F.when(F.col("brand").rlike("^(Stark|Globex)$"), "CORE").when(F.col("brand").rlike("^(Acme|Initech)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy(F.col("TH_item_count").asc_nulls_last(), "brand")
)
