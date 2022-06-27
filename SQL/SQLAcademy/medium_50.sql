-- Task 50
-- What percentage of students were born in 2000?
-- The result of rounding to the nearest whole in the smaller side.


SELECT 
  FLOOR(
    COUNT(DISTINCT(zoomer.student)) / 
    COUNT(DISTINCT(SiC.student)) * 100) as percent 
FROM 
  Student_in_class AS SiC, 
  (
    SELECT 
      student 
    FROM 
      Student_in_class SiC 
      INNER JOIN Student S ON SiC.student = S.id 
    WHERE 
      YEAR(birthday) = 2000
  ) AS zoomer
