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

    //




  }

}
