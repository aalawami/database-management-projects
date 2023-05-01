SELECT COUNT(DISTINCT User.user_id)
FROM (User
INNER JOIN Products ON User.user_id = Products.user_id)
WHERE rating > 1000;