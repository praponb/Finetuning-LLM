from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country", "city").orderBy("rating", "id")
w_ordered = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_avg_rating", "id")
w6 = Window.orderBy(F.col("running_count_rating").desc_nulls_first())
w7 = Window.orderBy(F.col("next_price_times_rating").asc_nulls_last())

filtered = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("London")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"), F.col("c.price"))
)

base = (
    with_latest
    .filter(F.col("rating") <= 2.8)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_supplier_name",
        "supplier_name_slug",
        "price",
        (F.col("price") * F.col("rating")).alias("price_times_rating"),
    )
)

enriched = (
    base
    .select(
        "id",
        "rating",
        "city_supplier_name",
        "supplier_name_slug",
        "price",
        F.lag("price_times_rating").over(w_grp).alias("prev_price_times_rating"),
        F.lead("price_times_rating").over(w_grp).alias("next_price_times_rating"),
        F.lead("price").over(w_grp).alias("next_price"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.ntile(4).over(w3).alias("rating_bucket"),
    )
)

enriched_suppliers = (
    enriched
    .select(
        "id",
        "supplier_name_slug",
        "prev_price_times_rating",
        "next_price_times_rating",
        "seq_no",
        F.min("rating").over(w_ordered).alias("running_min_rating"),
        F.max("rating").over(w_ordered).alias("running_max_rating"),
        F.count("rating").over(w_ordered).alias("running_count_rating"),
        F.avg("rating").over(w_ordered).alias("running_avg_rating"),
    )
)

kept = (
    enriched_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") == 123), "left_semi")
)

with_windows = (
    kept
    .select(
        "id",
        "supplier_name_slug",
        "prev_price_times_rating",
        "next_price_times_rating",
        "running_max_rating",
        "running_avg_rating",
        F.lead("running_min_rating", 2).over(w5).alias("next_running_min_rating"),
        F.rank().over(w6).alias("running_count_rating_rank"),
        F.percent_rank().over(w7).alias("next_price_times_rating_percent_rank"),
        F.sum("next_price_times_rating").over(w_ordered).alias("cumulative_next_price_times_rating"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("running_max_rating").alias("RUNNING_MAX_RATING"),
        F.col("next_price_times_rating").alias("NEXT_PRICE_TIMES_RATING"),
        F.col("running_avg_rating").alias("RUNNING_AVG_RATING_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["RUNNING_MAX_RATING", "NEXT_PRICE_TIMES_RATING", "RUNNING_AVG_RATING_VAL"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
