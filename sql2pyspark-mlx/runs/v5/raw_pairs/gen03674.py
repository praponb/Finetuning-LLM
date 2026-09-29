from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating_int", "id")
w2 = Window.orderBy("supplier_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("country").orderBy("rating", "id")
w5 = Window.partitionBy("city").orderBy("rating", "id")
w6 = Window.orderBy("next_supplier_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("running_max_rating_bucket").desc_nulls_last())
w9 = Window.orderBy(F.col("running_supplier_name_length").desc(), "id")
w10 = Window.orderBy("running_count_rating_bucket", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("rating") >= 2.4)
    .select(
        "*",
        F.length("supplier_name").alias("supplier_name_length"),
        F.col("rating").cast("int").alias("rating_int"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "rating_int",
        F.row_number().over(w).alias("event_seq"),
        F.sum("supplier_name_length").over(w2).alias("running_supplier_name_length"),
        F.ntile(3).over(w3).alias("rating_bucket"),
        F.lead("rating", 1, 0).over(w4).alias("next_rating"),
        F.lead("supplier_name_length", 1).over(w5).alias("next_supplier_name_length"),
    )
)

with_windows_suppliers = (
    with_windows
    .select(
        "*",
        F.min("rating_bucket").over(w6).alias("running_min_rating_bucket"),
        F.max("rating_bucket").over(w6).alias("running_max_rating_bucket"),
        F.count("rating_bucket").over(w6).alias("running_count_rating_bucket"),
    )
)

with_windows_suppliers_2 = (
    with_windows_suppliers
    .select(
        "id",
        "rating_int",
        "running_supplier_name_length",
        "rating_bucket",
        "next_rating",
        "running_min_rating_bucket",
        "running_count_rating_bucket",
        F.avg("running_count_rating_bucket").over(w7).alias("running_avg_running_count_rating_bucket"),
        F.count("running_count_rating_bucket").over(w7).alias("running_count_running_count_rating_bucket"),
        F.max("running_count_rating_bucket").over(w7).alias("running_max_running_count_rating_bucket"),
        F.min("running_count_rating_bucket").over(w7).alias("running_min_running_count_rating_bucket"),
        F.cume_dist().over(w8).alias("running_max_rating_bucket_cume_dist"),
    )
)

scored = (
    with_windows_suppliers_2
    .select(
        "id",
        "rating_int",
        "running_supplier_name_length",
        "rating_bucket",
        "running_min_rating_bucket",
        "running_avg_running_count_rating_bucket",
        "running_max_running_count_rating_bucket",
        "running_min_running_count_rating_bucket",
        "running_max_rating_bucket_cume_dist",
        F.row_number().over(w9).alias("seq_no"),
        F.avg("running_supplier_name_length").over(w10).alias("running_supplier_name_length_avg_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "rating_int",
        "running_supplier_name_length",
        "rating_bucket",
        "running_avg_running_count_rating_bucket",
        "running_max_running_count_rating_bucket",
        "running_min_running_count_rating_bucket",
        "running_max_rating_bucket_cume_dist",
        "seq_no",
    )
    .orderBy(F.col("rating_int").asc_nulls_first())
)
