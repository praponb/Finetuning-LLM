from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("launch_date", "id")
w3 = Window.partitionBy("country").orderBy(F.col("launch_date").desc(), "id")
w4 = Window.partitionBy("country").orderBy("seq_no")
w5 = Window.orderBy("row_seq")
w6 = Window.partitionBy("country")

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", "s.country", F.col("c.launch_date"))
)

with_windows = (
    latest_match
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("row_seq_3", F.row_number().over(w2))
)

remaining_keys = (
    with_windows
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(~F.col("city").isin(["Singapore", "London", "Berlin"]))
        .select("country", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") > 280), "left_anti")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty").between(154, 388)), "left_anti")
)

kept = (
    not_flagged_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= F.col("s.launch_date")), "left_anti")
)

enriched = (
    kept
    .filter(F.col("seq_no") != 5)
    .select(
        "supplier_name",
        "country",
        "seq_no",
        "row_seq_2",
        F.rank().over(w4).alias("seq_no_rank"),
        F.rank().over(w5).alias("row_seq_rank"),
        F.round((F.col("row_seq_3") / F.sum("row_seq_3").over(w6)), 4).alias("row_seq_3_share"),
        F.round((F.col("row_seq_3") / F.sum("row_seq_3").over(w6)), 4).alias("row_seq_3_share_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("row_seq_3_share_2") >= 0.73, "HIGH").otherwise("LOW").alias("row_seq_3_share_2_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.avg("row_seq_3_share").alias("avg_row_seq_3_share"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
    )
    .orderBy("th_rows")
)
