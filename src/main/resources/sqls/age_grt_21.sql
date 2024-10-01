SELECT c.id, c.first_name, c.last_name, c.company, c.city,c.age FROM customers c LEFT JOIN
customers d ON c.id=d.id
 WHERE c.age > 21