WITH age_grt_22 AS (
    SELECT c.id, c.first_name, c.last_name, c.company, c.city
    FROM age_grt_24 c
    WHERE c.age > 21
)
SELECT c.first_name, c.last_name, c.company, c.city FROM age_grt_21 c LEFT JOIN age_lt_18 c1 ON c.id = c1.id