
import java.io.{BufferedWriter, File, FileWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable
import scala.io.Source

object Lab1 {
  val selectedFilmRatings: mutable.MutableList[Int] = mutable.MutableList[Int]()
  val allFilmsRatings: mutable.MutableList[Int] = mutable.MutableList[Int]()

  val selectedFilmSortedRatings: mutable.MutableList[Int] = mutable.MutableList[Int]()
  val allFilmsSortedRatings: mutable.MutableList[Int] = mutable.MutableList[Int]()

  val filmId = 79
  val marks = List(1,2,3,4,5)

  def readData(): Unit = {
    val bufferedSource = Source.fromResource("u.data").getLines()
    bufferedSource.map { x =>
      //разделям строку на содержащиеся в ней элементы
      val value = x.split("\t")
      (value(0).toInt, value(1).toInt, value(2).toInt, value(3).toInt)
    }.foreach { z =>
      //записываем оценки по всем фильмам
      allFilmsRatings += z._3
      //отбираем rating по нужному нам item_id
      if (z._2 == filmId){
        selectedFilmRatings += z._3
      }
    }
  }

  def countMarks(unsortedMarkList: mutable.MutableList[Int],sortedMarkList: mutable.MutableList[Int]): Unit = {
    for (mark <- marks){
      val markNumber = unsortedMarkList.groupBy(identity).mapValues(_.size)(mark)
      println(mark +  ":  " + markNumber)
      sortedMarkList += markNumber
    }
  }

  def generateJSON(fileName: String): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val root: ObjectNode = mapper.createObjectNode()

    val jsonSelectedFilmList: ArrayNode = mapper.valueToTree(selectedFilmSortedRatings)
    root.putArray("hist_film").addAll(jsonSelectedFilmList)

    val jsonAllFilmsList: ArrayNode = mapper.valueToTree(allFilmsSortedRatings)
    root.putArray("hist_all").addAll(jsonAllFilmsList)

    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(root.toString)
    bw.close()
  }

  def main(args: Array[String]): Unit = {

    //читаем файл с данными
    readData()
    println("we got selected film marks: " + selectedFilmRatings.length)

    //считаем количество оценок по заданному фильму
    countMarks(selectedFilmRatings,selectedFilmSortedRatings)

    println("we got all films marks: " + allFilmsRatings.length)

    //считаем количество оценок по всем фильмам
    countMarks(allFilmsRatings,allFilmsSortedRatings)

    //создаем JSON  и пишем его в файл
    generateJSON("lab01.json")
  }
}
