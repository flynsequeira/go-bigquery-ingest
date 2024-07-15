CREATE blockdataproject.blockdata.transaction_vol_aggregate AS (
    SELECT 
        date,
        project_id, 
        COUNT(volume) AS total_transactions,
        SUM(volume_usd) AS total_volume_in_usd 
    FROM `blockdataproject.blockdata.s1`
    GROUP BY date, project_id;
);