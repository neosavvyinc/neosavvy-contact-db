import java.sql.{DriverManager, ResultSet}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._

import scala.annotation.tailrec
import scala.io.StdIn

final case class Person(
           firstName: String,
           middleName: String,
           lastName: String,
           email: String,
           phone: String,
           linkedInUrl: String,
           websiteUrl: String)

final case class People( people : List[Person])

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val personFormat = jsonFormat7(Person)
  implicit val peopleFormat = jsonFormat1(People)

//  implicit object ListTypeFormat extends JsonFormat[List[Person]] {
//    override def write(obj : List[Person]) : JsValue = JsArray(obj.map(personFormat.write))
//
//    override def read(json : JsValue) : List[Person] = json match {
//      case JsArray(x) => x.map(personFormat.read)
//      case _          => deserializationError("Expected String value for List[Person]")
//    }
//  }
}

/**
  * Created by aparrish on 4/20/17.
  */
object ContactServiceApp extends Directives with JsonSupport {
  def resultSetItr(resultSet: ResultSet): Stream[ResultSet] = {
    new Iterator[ResultSet] {
      def hasNext = resultSet.next()
      def next() = resultSet
    }
  }.toStream

  @tailrec
  def collectRows(resultSet: ResultSet, acc: List[Person]) : List[Person] = {
    val hasNext = resultSet.next()
    if(hasNext) {
      collectRows(resultSet, acc ::: List(Person(
        resultSet.getString(1),
        resultSet.getString(2),
        resultSet.getString(3),
        resultSet.getString(4),
        resultSet.getString(5),
        resultSet.getString(6),
        resultSet.getString(7)
      )))
    } else {
      acc
    }
  }

  def main(args: Array[String]) {

    implicit val system = ActorSystem("ns-contact-service-system")
    implicit val materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher

    val findUsers = path("hello") {
      get {

        classOf[org.postgresql.Driver]
        val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/neosavvy_contacts?user=aparrish")

        try {

          val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          val selectPeople =
            s"""
                                   | select
                                   |     first_name,
                                   |     middle_name,
                                   |     last_name,
                                   |     email,
                                   |     phone,
                                   |     linked_in_url,
                                   |     website_url
                                   | from
                                   |     person
            """.stripMargin
          val rs = stm.executeQuery(selectPeople)
//          val rsItr = resultSetItr(rs)
//
//          val people = rsItr.map(row => {
//            Person(
//              row.getString(1),
//              row.getString(2),
//              row.getString(3),
//              row.getString(4),
//              row.getString(5),
//              row.getString(6),
//              row.getString(7)
//            )
//          })

          //http://scalikejdbc.org/?

          // iterate through the result set

          // return a mapped Person for each item in the result set

//          val peopleAsList = People(collectRows(rs, List()))

          complete(collectRows(rs, List()))
        }
        catch {
          case e: Exception => {
            println(e.toString)
            complete("Failed with Error")
          }
        }
        finally {
          conn.close()
        }
      }
    }

    val addContact = path("hello") {
      post {
        entity(as[Person]) { person =>

          classOf[org.postgresql.Driver]
          val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/neosavvy_contacts?user=aparrish")

          try {

            val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
            val insertStatement = s"""
                                     | INSERT INTO person
                                     | (
                                     |   first_name,
                                     |   middle_name,
                                     |   last_name,
                                     |   email,
                                     |   phone,
                                     |   linked_in_url,
                                     |   website_url
                                     | )
                                     | values (
                                     |   '${person.firstName}',
                                     |   '${person.middleName}',
                                     |   '${person.lastName}',
                                     |   '${person.email}',
                                     |   '${person.phone}',
                                     |   '${person.linkedInUrl}',
                                     |   '${person.websiteUrl}'
                                     | )
                                     | returning id
            """.stripMargin
            println(insertStatement);
            val rs = stm.execute(insertStatement)

          } finally {
            conn.close()
          }


          println(s"Person.name: ${person.firstName}")
          println(s"Person.name: ${person.middleName}")
          println(s"Person.name: ${person.lastName}")
          println(s"Person.name: ${person.email}")
          println(s"Person.name: ${person.phone}")
          println(s"Person.name: ${person.linkedInUrl}")
          println(s"Person.name: ${person.websiteUrl}")

          complete(person)
        }
      }
    }

    val route = addContact ~ findUsers

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

}
