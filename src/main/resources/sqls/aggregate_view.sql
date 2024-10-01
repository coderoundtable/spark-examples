WITH age_grt_22 AS (
    SELECT c.id, c.first_name, c.last_name, c.company, c.city
    FROM age_grt_21 c
    WHERE c.age > 21
)
SELECT city, count(*) FROM age_grt_22
GROUP BY city