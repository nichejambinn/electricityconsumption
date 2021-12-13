// maximum hourly energy consumption for each house
val lines = sc.textFile("/ecoFlume/*/*/*.log")
val records = lines.map(ln => ln.split(","))

// map -> key (String): "<house_id> <date> <hour>", val (Double): energy
val pairs = records.map(rec => (
    rec(1) + " " + rec(2) + " " + rec(3).split(":")(0)
    , rec(4).toDouble
))

pairs.cache()

// get min and max energy reading for each key
val minEnrg = pairs.reduceByKey((x, y) => Math.min(x, y))
val maxEnrg = pairs.reduceByKey((x, y) => Math.max(x, y))

// find the difference between each to get the total consumption for that hour
val joinMaxMin = maxEnrg.join(minEnrg)
val diffEnrg = joinMaxMin.mapValues(v => v._1 - v._2)

// remap keys to the house id only
val houseDiff = diffEnrg.map(rec => (
    rec._1.split(" ")(0)
    , rec._2
))

// get the maximum total hourly consumption for each house
val maxPerHouse = houseDiff.reduceByKey((x,y) => Math.max(x,y))

// display results
maxPerHouse.collect().foreach(println)
