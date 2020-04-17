package com.example.gatling

import com.example.graph.GraphNodeEntity.{Tags, To}
import com.example.graph.http.Requests.{CreateNodeReq, UpdateEdgeReq}
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.core.body.StringBody
import io.gatling.core.session.Session
import io.gatling.http.Predef._
import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Formats, jackson, native}

import scala.concurrent.duration._
import scala.util.Random

class GraphScenario extends Simulation {
  private val config = ConfigFactory.load()

  implicit def asFiniteDuration(d: java.time.Duration) =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  implicit val serialization = jackson.Serialization
  implicit val formats: Formats = DefaultFormats

  val baseUrl = config.getString("loadtest.baseUrl")
  val rampupUsers = config.getInt("loadtest.rampup.users")
  val rampupTime = config.getDuration("loadtest.rampup.time")

  val httpConf = http.baseUrl(baseUrl).shareConnections.contentTypeHeader("application/json")

  val rand = new Random()

  val nodeIdFeeder = csv("nodeId.csv")
  val nodeTypeFeeder = csv("nodetype.csv").random
  val tagsFeeder = csv("tags.csv").random
  val tagSeq = Seq(
    "huawei",
    "ericsson",
    "lenovo",
    "nokia",
    "zte",
    "google",
    "JeffDean",
    "Ren",
    "ChinaTelecom",
    "Qualcom",
    "ChinaUnicom",
    "vivo",
    "ChinaMobile",
    "GSMA",
    "Oppo",
    "CICT"
  )

  def nodeReq(session: Session): CreateNodeReq = {
    val nodeId = session("nodeId").as[String]
    println(nodeId)
    val x = nodeId.substring(1,2)
    val y = nodeId.substring(3,4)
    println(s"($x, $y)")
    val nodetype = session("nodetype").as[String]

    val tags = Seq(nodetype, tagSeq(rand.nextInt(15)), tagSeq(rand.nextInt(15)))
    val tagMap = tags.map(_ -> (rand.nextInt(5) + 2)).toMap

    CreateNodeReq(
      nodeId,
      nodetype,
      tagMap,
      Map("x" -> x, "y" -> y)
    )
  }

  val nodeScn = scenario("create nodes")
    .feed(nodeIdFeeder)
    .feed(nodeTypeFeeder)
    .exec(
      http("create node")
        .put("/api/graph")
        .body(new StringBody(session => write(nodeReq(session))))
        .check(status.is(200))
    )

  val edgeTypeFeeder = csv("edgeType.csv").random
  val edgeNodeIdFeeder = csv("nodeId.csv").random
  val targetNodeIdFeeder = csv("targetNodeId.csv").random
  val userIdFeeder = csv("userIds.csv").random

  def edgeReq(session: Session) = {
    val nodeId = session("nodeId").as[String]
    var targetNodeId = session("targetNodeId").as[String]
    val edgeType = session("edgeType").as[String]

    if(nodeId == targetNodeId) {
      targetNodeId = "Earth"
    }
    write(
      UpdateEdgeReq(
        targetNodeId,
        edgeType,
        "TO",
        session("userId").as[String],
        Map(
          "distance" -> session("distance").as[String],
          "orbitalperiod" -> session("orbitalperiod").as[String]
        )
      )
    )
  }

  val edgeScn = scenario("create edges")
    .feed(edgeTypeFeeder)
    .feed(edgeNodeIdFeeder)
    .feed(targetNodeIdFeeder)
    .exec(
      http("create an edge")
        .post("/api/graph/${nodeId}/edge")
        .body(new StringBody(session => edgeReq(session)))
        .check(status.is(200))
    )

  setUp(
    nodeScn.inject(rampUsers(80) during (20 seconds))
  ).protocols(httpConf)

//  setUp(
//    edgeScn.inject(rampUsers(150) during (10 seconds))
//  ).protocols(httpConf)
}
