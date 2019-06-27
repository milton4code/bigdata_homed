package com.examples.scalatest

class Foo(val name: String, val age: Int, val sex: Symbol)
 
object Foo {
  def apply(name: String, age: Int, sex: Symbol) = new Foo(name, age, sex)
}