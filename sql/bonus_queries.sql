WITH top_regions AS (
    SELECT region
    FROM trips
    GROUP BY region
    ORDER BY COUNT(*) DESC, region ASC
    LIMIT 2
)
SELECT DISTINCT ON (region)
    region,
    datasource,
    trip_ts AS latest_trip_ts
FROM trips
WHERE region IN (SELECT region FROM top_regions)
ORDER BY region, trip_ts DESC;

SELECT DISTINCT region
FROM trips
WHERE datasource = 'cheap_mobile'
ORDER BY region;
