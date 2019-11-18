class Person (var name: String, var age: Int)

class Person1(val name: String, var age: Int)



//Inheritance
class Shape(val x: Int, val y: Int) {
  val isAtOrigin: Boolean = x == 0 && y == 0
}

class Rectangle(val x: Int, y: Int, val width: Int, val height: Int)
  extends Shape(x, y)

class Square(x: Int, y: Int, width: Int)
  extends Rectangle(x, y, width, width)

class Circle(x: Int, y: Int, val radius: Int)
  extends Shape(x, y)

val rect = new Rectangle(x = 0, y = 3, width = 3, height = 2)
rect.x
rect.y
rect.isAtOrigin
rect.width
rect.height



//Constructor overloading


class Animal (var name: String, var age: Int) {

  // (2) auxiliary constructor
  def this (name: String) {
    this(name, 0)
  }
  override def toString = s"$name is $age years old"
}


// calls the Animal one-arg constructor
class Dog (name: String) extends Animal (name) {
  println("Dog constructor called")
}

class Student(id:Int, name:String){
  var age:Int = 0
  def showDetails(){
    println(id+" "+name+" "+age)
  }
  def this(id:Int, name:String,age:Int){
    this(id,name)       // Calling primary constructor, and it is first line
    this.age = age
  }
}


// Overriding method

class Shape1(val x: Int, val y: Int) {
  def description: String = s"Shape at (" + x + "," + y + ")"
}

class Rectangle1(x: Int, y: Int, val width: Int, val height: Int)
  extends Shape1(x, y) {
  override def description: String = {
    super.description + s" - Rectangle " + width + " * " + height

  }
  def descThis: String = this.description
  def descSuper: String = super.description
}

val rect1 = new Rectangle1(x = 0, y = 3, width = 3, height = 2)
rect1.description


//trait

trait Description {
  def description: String
}