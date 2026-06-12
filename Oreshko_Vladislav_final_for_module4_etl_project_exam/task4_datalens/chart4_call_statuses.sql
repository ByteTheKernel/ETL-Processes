SELECT call_status, COUNT(*) AS total_calls
FROM `etl-exam-source`.`raw/transactions_v2.csv`
WITH (
    FORMAT="csv_with_names",
    SCHEMA=(
        call_id Utf8,
        call_time Utf8,
        client_id Utf8,
        region_code Utf8,
        campaign_type Utf8,
        call_status Utf8,
        client_response Utf8,
        duration_sec Int32,
        follow_up_required Utf8
    )
)
GROUP BY call_status
ORDER BY total_calls DESC