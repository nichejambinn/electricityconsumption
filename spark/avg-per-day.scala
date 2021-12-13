// average daily energy consumption for each house
val lines = sc.textFile("/ecoFlume/*/*/*.log")
val records = lines.map(ln => ln.split(","))

// map -> key (String): "<house_id> <date>", val (Double): energy
val pairs = records.map(rec => (
    rec(1) + " " + rec(2)
    , rec(4).toDouble
))

pairs.cache()

// get min and max energy reading for each key
val minEnrg = pairs.reduceByKey((x, y) => Math.min(x, y))
val maxEnrg = pairs.reduceByKey((x, y) => Math.max(x, y))

// find the difference between each to get the total consumption for that day
val joinMaxMin = maxEnrg.join(minEnrg)
val diffEnrg = joinMaxMin.mapValues(v => v._1 - v._2)

// remap keys to the house id and add a counter variable to the value
val houseDiffAndCount = diffEnrg.map(rec => (
    rec._1.split(" ")(0)
    , (rec._2, 1)
))

// sum the energy consumption and number of days for each house
val houseSum = houseDiffAndCount.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

// take the average by dividing the consumption sum over the number of days sum
val avgPerHouse = houseSum.mapValues(v => v._1 / v._2)

// display the result
avgPerHouse.collect().foreach(println)
