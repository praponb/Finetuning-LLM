from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("session_id", "page").orderBy("seq_no", "id")
w4 = Window.partitionBy("page").orderBy(F.col("next_duration_sec").asc_nulls_first())
w5 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc_nulls_first())

enriched = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-08-23")
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked_web_events = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked_web_events
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(150, 376))
        .select("page")
    )
)

reconciled = (
    ranked_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("next_duration_sec_dense_rank", F.dense_rank().over(w4))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w5))
)

kept_keys_2 = (
    scored
    .select("session_id", "page")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id").isin([5, 8, 17]))
        .select("session_id", "page")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

by_type = (
    kept_rows
    .groupBy("event_type")
    .pivot("page", ["/home", "/product"])
    .agg(
        F.max("duration_sec_percent_rank").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
    .select(
        "event_type",
        F.col("/home_value_sum").alias("home_value_sum"),
        F.col("/home_n").alias("home_n"),
        F.col("/product_value_sum").alias("product_value_sum"),
        F.col("/product_n").alias("product_n"),
    )
)

result = (
    by_type
    .select(
        "event_type",
        "home_value_sum",
        "home_n",
        "product_value_sum",
        "product_n",
        F.when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "OTHER_ZONE").when(F.col("event_type").rlike("^(click|purchase)$"), "GROUP_A").otherwise("CORE").alias("event_type_zone"),
    )
)
