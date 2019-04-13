from neo4j.v1 import GraphDatabase, basic_auth

#connection with authentication
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "cs1656"), encrypted=False)

#connection without authentication
#driver = GraphDatabase.driver("bolt://localhost:7687", encrypted=False)

file = open("output.txt", "w", encoding="utf-8")


session = driver.session()
transaction = session.begin_transaction()

result = transaction.run("""MATCH (a:Actor) -[:ACTS_IN]-> () RETURN a.name, count(*) as cnt ORDER BY cnt DESC LIMIT 20""")
file.write("### Q1 ###\n")
for record in result:
   file.write ("{}, {}\n".format(record["a.name"], record["cnt"]))
file.write("\n")

result = transaction.run("""MATCH (m:Movie) <-[r:RATED]- (:Person) WHERE r.stars <= 3 Return m.title""")
file.write("### Q2 ###\n")
for record in result:
   file.write ("{}\n".format(record["m.title"]))
file.write("\n")

result = transaction.run("""MATCH (m:Movie) WITH  m, size(() -[:ACTS_IN]->(m)) as numactors ORDER BY numactors DESC LIMIT 1 Return m.title, numactors""")
file.write("### Q3 ###\n")
for record in result:
   file.write ("{}, {}\n".format(record["m.title"], record["numactors"]))
file.write("\n")

result = transaction.run("""MATCH (a:Actor) -[:ACTS_IN]->(:Movie)<-[:DIRECTED]-(d:Director) WITH a, count(DISTINCT d) as cnt WHERE cnt >= 3 RETURN a.name, cnt""")
file.write("### Q4 ###\n")
for record in result:
   file.write ("{}, {}\n".format(record["a.name"], record["cnt"]))
file.write("\n")

result = transaction.run("""MATCH (a:Actor {name: 'Kevin Bacon'})-[:ACTS_IN*4]-(a2:Actor) WHERE NOT (a)-[:ACTS_IN]->()<-[:ACTS_IN]-(a2) RETURN DISTINCT a2.name""")
file.write("### Q5 ###\n")
for record in result:
   file.write ("{}\n".format(record["a2.name"]))
file.write("\n")

result = transaction.run("""MATCH (a:Actor {name: 'Tom Hanks'})-[:ACTS_IN]-(m:Movie) RETURN DISTINCT m.genre""")
file.write("### Q6 ###\n")
for record in result:
   file.write ("{}\n".format(record["m.genre"]))
file.write("\n")

result = transaction.run("""MATCH (d:Director)-[:DIRECTED]->(m:Movie) WITH d.name as dn, count(distinct m.genre) as cnt WHERE cnt >= 2 RETURN dn, cnt""")
file.write("### Q7 ###\n")
for record in result:
   file.write ("{}, {}\n".format(record["dn"], record["cnt"]))
file.write("\n")

result = transaction.run("""MATCH (d:Director)-[:DIRECTED]->()<-[:ACTS_IN]-(a:Actor) WITH d, a, count(*) as cnt RETURN d.name, a.name, cnt ORDER BY cnt DESC LIMIT 5""")
file.write("### Q7 ###\n")
for record in result:
   file.write ("{}, {}, {}\n".format(record["d.name"], record["a.name"], record["cnt"]))
file.write("\n")

transaction.close()
session.close()