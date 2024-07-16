object assignment {
  def main(args:Array[String]):Unit= {

    val a: Int = 14

    if (a % 2 == 0 && a >= 0) {
      println(a, "is even and positive")
    }
    else {
      println(a, "is not even and positive")
    }

    val b = -15

    if (b < -10 || b > 10) {
      println(b, "is less than minus ten")
    }
    else {
      println(b, "is neither less than minus ten or greater than ten")
    }

    val c: Int = 27

    if (c % 2 != 0 && c % 3 != 0) {
      println(c, "is odd and not divisible by 3")
    }
    else {
      println(c, "is odd and divisible by three")
    }

    val d: Int = 18

    if (d % 4 == 0 || d % 6 == 0) {
      println(d, "is divisible by either 4 or 6")
    }
    else {
      println(d, "is not divisible by either 4 or 6")
    }


    val age = 20

    if (age >= 18 || age >= 16) {
      println("eligible to vote or drive")
    }
    else {
      println("not eligible to vote or drive")
    }

    val e = 25

    if ((1 to 10 contains e) || (20 to 30 contains e)) {
      println(e, "is in range")
    }
    else {
      println(e, "is not in range")
    }

    val f = -7
    if (f <= -1 && f % 2 != 0) {
      println(f, "is negative and odd")
    }
    else {
      println(f, "is nor negative and odd")
    }

    val g = 63
    if (g > 60) {
      println("Senior citizen discount")
    }
    else if (g < 25) {
      println("student discount")
    }
    else {
      println("none of the above")
    }

    val h = 35
    if (h % 5 == 0 && h % 7 == 0) {
      println(h, "is divisible by 5 and 7")
    }
    else {
      println(h, "is not divisible by 5 and 7")
    }

    val i = -8
    if (i < 0 || i % 2 == 0) {
      println(i, "is negative or even")
    }
    else {
      println(i, "is not negative or even")
    }

    val j = 17
    if (j <= 1) {
      print(j, "is not a prime number")
    }
    else if (j % 2 == 0 || j % 3 == 0 || j % 5 == 0 || j % 7 == 0) {
      println(j, "is not a prime number")
    }
    else {
      println(j, "is a prime number")
    }

    val k = 120
    if (k > 150) {
      println("customer is eligible for discount and free shipping")
    }
    else if (k > 100 || k < 150) {
      println("customer is eligible for free shipping")
    }
    else {
      println("customer not eligible for discount or free shipping")
    }

    val l = 24
    if (l % 3 == 0 || l % 8 == 0) {
      println(l, "is divisible by 3 or 8")
    }

    val m = -6
    if (m < 0 && m % 2 == 0) {
      println(m, "is non positive and even")
    }
    else {
      println(m, "is not none positive and even")
    }

    val n = 15
    if (n < 13) {
      println("child")
    }
    else if (n >= 13 || n <= 19) {
      println("teenager")
    }
    else if (n >= 20) {
      println("adult")
    }
    else {
      println("not in age group definition")
    }

    val o = 25
    if (o % 2 == 0 || o % 5 == 0) {
      println(o, "is divisible by 2 or 5")
    }
    else {
      println("noot divisible by 2 or 5")
    }

    val p = 70
    if (p > 60 && p < 25) {
      println("eligible for senior citizen and student discount")
    }
    else {
      println("not eligible for senior citizen and student discount")
    }

    val q = 21
    if (q % 3 == 0 && q % 7 == 0) {
      println(q, "is a multiple of 3 and 7")
    }
    else {
      println(q, "is not a multiple of 3 and 7")
    }

    val r = 45
    if (r % 5 == 0 || r % 9 == 0) {
      println(r, "is divisible by either 5 or 9")
    }
    else {
      println(r, "is not divisible by either 5 or 9")
    }

    val s = 15
    if (s % 2 != 0 && s % 4 != 0) {
      println(s, "is odd and not divisible by 4")
    }
    else {
      println(s, "is not odd and not divisible by 4")
    }

    val t = 15
    if (t % 3 == 0 && t % 5 == 0) {
      println(t, "is divisible by both 3 qnd 5")
    }
    else {
      println(t, "is not divisible by both 3 and 5")
    }


    val purchase_amount = 180
    val loyalty_card = true
    if (purchase_amount > 180 || loyalty_card) {
      println("customer is eligible for discount or membership benefit")
    }
    else {
      println("customer is not eligible for discount or membership benefit")
    }

    val u = 9
    if (u % 2 == 0 || u % 3 == 0) {
      println(u, "is divisible by either 2 or 3")
    }
    else {
      println(u, "is not divisible by either 2 or 3")
    }

    val v = 7
    if (v > 0 && v % 3 != 0) {
      println(v, "is positive and not divisible by 3")
    }
    else {
      println(v, "does not fulfil the condition")
    }

    val w = 70
    val new_customer = false
    val is_not_a_new_customer = true
    if (w > 65 && is_not_a_new_customer) {
      println("eliigible for senior citizen discont")
    }
    else if (w > 65 && new_customer) {
      println("not eligible for senior citizen discount")
    }
    else {
      println("not eligiblr for discount")
    }

    val x = 11
    if (x % 2 != 0 || x % 3 != 0 || x % 5 != 0 || x % 7 != 0) {
      println(x, "is odd or prime number")
    }
    else {
      println(x, "is not odd or prime")
    }

    val y = 120
    if (y > 150 && y > 100) {
      println("eligible for fiscount and freee shipping")
    }
    else {
      println("not eligible for discount and free shipping")
    }

    val z = 14
    if (z > 0 && z % 7 != 0) {
      println(z, "is non negative and  not divisible by 7")
    }
    else {
      println(z, "is non negative but divisible by 7")
    }

    val free_trial = true
    val student_age = 22
    if (student_age < 26 || free_trial) {
      println("eligible for student discount or free trial")
    }
    else {
      println("not eligible for student discount or free trial")
    }

    val number = 24
    if (number % 4 == 0 || number % 6 == 0) {
      println(number, "is divisible by 4 or 6")
    }
    else (number, "is not divisible by either 4 or 6")

    for (i <- 1 to 5) {
      println(i)
    }

    for (i <- 1 to 5) {
      println(i * i)
    }

    for (i <- 1 to 15)
      if (i % 3 == 0) {
        println(i)
      }
    // sum of number
    var num = 231
    var rem = 0
    var sum = 0

    while (num != 0) {
      rem = num % 10
      sum = sum + rem
      num = num / 10
    }
    println(sum)
    //palindrome
    var numm = 231
    var bb = 231
    var summ = 0
    var remm = 0

    while (numm != 0) {
      remm = numm % 10
      summ = summ * 10 + rem
      numm = numm / 10
    }

    if (b == summ) {
      println("palindrome number")
    }

    else {
      println("not a palindrome")
    }

    //even numbers from 2 to 10
    var start = 2
    while (start <= 10) {
      if (start % 2 == 0) {
        println(start)
      }
      start = start + 1
    }

    //sum of all numbers from 1 to 50

    val first = 1
    val last = 50
    val numbers = 50

    def summation = numbers / 2 * (first + last)

    println(summation)

    // square of numbers from 1 to 5
    for (square <- 1 to 5) {
      println(square * square)
    }

    //square of numbers from 1 to 5 using while loop
    var square = 1
    while (square <= 5) {
      println(square * square)
      square = square + 1
    }

    //first multiples of 3 using for loop
    for (i <- 1 to 9) {
      if (i % 3 == 0)
        println(i)
    }

    //odd numbers from 1 to 15 using while loop
    var odd = 1
    while (odd <= 15) {
      if (odd % 2 != 0)
        println(odd)
      odd = odd + 1
    }


  }
}