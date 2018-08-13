// transformation and actions
val inp = sc.parallelize(List(1,2,3,4))
val res = inp.map(x => x*x)
println(res.collect().mkString(",")) //This is an action
