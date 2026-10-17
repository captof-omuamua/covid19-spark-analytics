import org.apache.spark.SparkContext

object bda {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "project")

    val data = sc.textFile("C:\\Users\\LOCAL_USER\\Desktop\\4th Sem P\\BDA\\Term Project\\country_wise_latest.csv\\")
      .map(x => x.split(","))
      .zipWithIndex()
      .filter(_._2 > 0)
      .map(_._1)

    val modified = data.collect()
    //modified.foreach(x => println(x.mkString("  ")))
    val a = modified.flatMap(col => Array(col(0)))
    val region = "Americas"

    high_confirmed(modified,a)
    total_numdeaths(modified)
    Mortality_Rate(modified, a)
    Recovery_Rate(modified, a)
    significant_increase(modified, a)
    println()
    val rt = whoregion(modified)
    println(rt.mkString(","))
    println()
    regioncountries(modified)
    regionbased(modified, region)
    println()

    val recoveryrate = recoveryregion(modified, rt)
    val sortedList = recoveryrate.sortBy(_._2).reverse.take(1)
    sortedList.foreach(println)
  }

  def high_confirmed(modified:Array[Array[String]],a:Array[String]): Unit = {

    val ab = modified.flatMap(col => Array(col(1).toInt))
    val max_casesRDD = ab.max
    println(s"${a(ab.indexOf(max_casesRDD))} country has maximum ${max_casesRDD} cases among all Countries.")

    println()
  }

  def total_numdeaths(modified:Array[Array[String]]): Unit ={

    val b = modified.flatMap(col => Array(col(2).toInt))
    val deathsRDD = b.reduce(_ + _)
    println(s"Total number of deaths among all countries $deathsRDD")
    println()
  }

  def Mortality_Rate(modified: Array[Array[String]], a: Array[String]): Unit = {
    val confirmedCasesRDD =modified.flatMap(col => Array(col(1).toInt))
    val deathsRDD = modified.flatMap(col => Array(col(2).toInt))

    val mortalityRatesRDD = confirmedCasesRDD.zip(deathsRDD).map {
      case (confirmed, deaths) =>  deaths.toDouble / confirmed.toDouble
    }

    val countriesRDD = mortalityRatesRDD.zip(a)

    val maxiCountryRDD = countriesRDD.maxBy(_._1)._2
    val miniCountryRDD = countriesRDD.minBy(_._1)._2

    println(s"Country has Maximum Morality Rate: $maxiCountryRDD")
    println(s"County has Minimum Morality Rate: $miniCountryRDD")
    println()

  }

  def Recovery_Rate(modified: Array[Array[String]], a: Array[String]): Unit = {

    val confirmRDD = modified.flatMap(col => Array(col(1).toInt))
    val recoverdRDD = modified.flatMap(col => Array(col(3).toInt))

    val recoveryrateRDD = confirmRDD.zip(recoverdRDD).map {
      case (confirmedRDD, recoveredRDD) => recoveredRDD / confirmedRDD
    }

    val countriesRDD = recoveryrateRDD.zip(a)

    val maxiCountryRDD = countriesRDD.maxBy(_._1)._2
    val miniCountryRDD = countriesRDD.minBy(_._1)._2

    println(s"Country with the highest Recovery Rate: $maxiCountryRDD")
    println(s"Country with the lowest Recovery Rate: $miniCountryRDD")
    println()

  }

  def significant_increase(modified: Array[Array[String]], a: Array[String]): Unit = {
    val newcasesRDD = modified.flatMap(col => Array(col(5).toInt))
    val newdeathsRDD = modified.flatMap(col => Array(col(6).toInt))

    val casesRDD = newcasesRDD.zip(a).filter(_._1 > 1000)
    val deathsRDD = newdeathsRDD.zip(a).filter(_._1 > 500)

    println("Significance increase in new cases:")

    val matchingCountries = casesRDD.flatMap {
      case (_, country) => a.find(_.contains(country))
    }
    println(matchingCountries.mkString(","))

    println()

    println("Significance increase in new deaths:")
    val count = deathsRDD.flatMap {
      case (_, country) => a.find(_.contains(country))
    }
    println(count.mkString(","))

    println()
  }

  def whoregion(modified: Array[Array[String]]): Array[String] = {
    val regions = modified.map(col => col(10)).distinct
    println("The are WHO regions where all countries are divided acording to WHO regions: ")
    regions
  }

  def regioncountries(modified: Array[Array[String]]): Unit ={

    val j = modified.groupBy(col => col(10)).mapValues(_.map(_(0)))
    j.foreach { case (region, countries) =>
      println(s"WHO Region: $region")
      println("Countries: " + countries.mkString(", "))
      println()
    }
  }

  def regionbased(modified: Array[Array[String]], region:String): Unit ={

    val regionbased = modified.filter(col => col(10) == region)
    val a = regionbased.flatMap(col => Array(col(0)))
    println(s"There are total "+a.length + s" Countries in $region")

    val ab = regionbased.flatMap(col => Array(col(1).toInt))
    val max_cases = ab.max
    println(s"${a(ab.indexOf(max_cases))} country has maximum ${max_cases} cases among all Countries.")

    println()

    val average = ab.sum / ab.length
    println(s"The average number of cases among all the countries = ${average}")
    println()

    val above_average = regionbased.filter(col => col(1).toInt > average)
    val above_average_countries = above_average.map(col => col(0))
    println("Countries with confirmed cases above average:\n" + above_average_countries.mkString(","))
    println()

    val b = regionbased.flatMap(col => Array(col(2).toInt))
    val max = b.max
    println(s"${a(b.indexOf(max))} country has maximum ${max} deaths among all Countries.")

    println()

    val averageno = b.sum / b.length
    println(s"The average number of cases among all the countries = ${averageno}")
    println()

    val average_deaths = regionbased.filter(col => col(2).toInt > averageno)
    val average_countries = average_deaths.map(col => col(0))
    println("Countries with death cases above average:\n" + average_countries.mkString(","))
  }


  def recoveryregion(modified: Array[Array[String]], rt: Array[String]): List[(String, Double)] = {
    val regionsData = rt.map(region => {
      val regionData = modified.filter(row => row(10) == region)
      val regionConfirmed = regionData.flatMap(col => Array(col(1).toInt)).sum
      val regionRecovered = regionData.flatMap(col => Array(col(3).toInt)).sum
      val regionRecoveryRate = (regionRecovered.toDouble / regionConfirmed.toDouble) * 100
      (region, regionRecoveryRate)
    })

    println("This region has highest region: ")
    regionsData.toList
  }
}
