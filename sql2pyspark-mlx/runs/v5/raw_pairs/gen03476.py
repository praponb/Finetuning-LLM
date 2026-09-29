from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w_recent = Window.partitionBy("city", "region").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("opened_date", "id")
w_hist = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

base = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "New York"]))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
    .withColumn("opened_date_period", F.date_trunc("month", "opened_date"))
    .withColumn("region_clean", F.coalesce("region", F.lit("OTHER")))
)

metrics = (
    base
    .filter(F.to_date("opened_date_period") >= "2024-08-01")
    .select(
        "id",
        "region",
        "opened_date_label",
        "region_clean",
        F.row_number().over(w_grp).alias("seq_no"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.sum("sqft").over(w_recent).alias("cumulative_sqft"),
        F.lead("sqft", 1, 0).over(w4).alias("next_sqft"),
        F.sum("sqft").over(w_hist).alias("sqft_sum_90d"),
    )
)

kept_keys = (
    metrics
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("%Downtown%"))
        .select("id", "region")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([1, 2, 5, 8])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "region",
        "seq_no",
        "cumulative_sqft",
        "next_sqft",
        "sqft_sum_90d",
        F.when(F.col("region").rlike("^(South|East)$"), "CORE").when(F.col("region").rlike("^(North|West)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("region").desc())
)
