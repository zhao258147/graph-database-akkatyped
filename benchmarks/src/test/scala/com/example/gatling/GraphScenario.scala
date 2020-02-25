package com.example.gatling

import com.example.graph.GraphNodeEntity.To
import com.example.graph.http.Requests.{CreateNodeReq, UpdateEdgeReq}
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.core.body.StringBody
import io.gatling.core.session.Session
import io.gatling.http.Predef._
import org.json4s.native.Serialization.write
import org.json4s.{DefaultFormats, Formats, native}
import scala.concurrent.duration._
import scala.util.Random

class GraphScenario extends Simulation {
  private val config = ConfigFactory.load()

  implicit def asFiniteDuration(d: java.time.Duration) =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  val baseUrl = config.getString("loadtest.baseUrl")
  val rampupUsers = config.getInt("loadtest.rampup.users")
  val rampupTime = config.getDuration("loadtest.rampup.time")

  val httpConf = http.baseUrl(baseUrl).shareConnections.contentTypeHeader("application/json")

  val rand = new Random()

  val nodeIdFeeder = csv("nodeId.csv")
  val nodeTypeFeeder = csv("nodetype.csv").random

  val nodeScn = scenario("create nodes")
    .feed(nodeIdFeeder)
    .feed(nodeTypeFeeder)
    .exec(
      http("create node")
        .put("/api/graph")
        .body(new StringBody(session => write(CreateNodeReq(session("nodeId").as[String], session("nodetype").as[String]))))
        .check(status.is(200))
    )

  val edgeTypeFeeder = csv("edgeType.csv").random
  val edgeNodeIdFeeder = csv("nodeId.csv").random
  val targetNodeIdFeeder = csv("targetNodeId.csv").random

  def edgeReq(session: Session) = {
    val nodeId = session("targetNodeId").as[String]
    val edgeType = session("edgeType").as[String]
    write(UpdateEdgeReq(nodeId, edgeType, "TO"))
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
    nodeScn.inject(rampUsers(10) during (10 seconds))
  ).protocols(httpConf)

//  setUp(
//    edgeScn.inject(rampUsers(20) during (10 seconds))
//  ).protocols(httpConf)
}
