object sql_thing {
  def main(args:Array[String]):Unit={

   //question one
    SELECT employee.name
    FROM employees
    JOIN departments
    ON employees.dept_id = departments.dept_id
    WHERE dept_id = 102


    //question 2
    SELEECT name, dept_name
    FROM employees
    LEFT JOIN departments
    ON employees.dept_id = departments.dept_id



    //question 3
    SELECT departments.dept_name, employees.name
    FROM empployees
      RIGHT JOIN departments
    ON employees.dept_id = departments.dept_id


    //question 4
    SELECT name,, ddept_name
    FROM employees
      FULL JOIN departments
    on employees.dept_id = departments.dept_id

    //question 5
    SELECT a.name, b.dept_name
    FROM a.employee
    JOIN departments USING dept_id
      WHERE a.dept_id IN(
      SELECT a.dept_id
      FROM employees
        GROUP BY dept_id
        HAVING COUNT(name) > 1
    )

    //question 5
    SELECT a.name, b.dept_name
    FROM employees
      JOIN(
        SELECT dept_name
          FROM departments
          GROUP BY dept_id
        HAVING COUNT(name)> 1
      )
    USING dept_id




  }

}
