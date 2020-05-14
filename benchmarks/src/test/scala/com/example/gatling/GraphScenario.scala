package com.example.gatling

import com.example.graph.http.Requests.{CreateNodeReq, UpdateEdgeReq}
import com.example.saga.NodeRecommendationActor.NodeRecommendationSuccess
import com.example.saga.http.Requests.{NodeReferralReq, NodeVisitReq}
import com.example.user.UserNodeEntity.UserInfo
import com.example.user.http.Requests.CreateUserReq
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.core.body.StringBody
import io.gatling.core.session.Session
import io.gatling.http.Predef._
import org.json4s.jackson.Serialization.write
import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, Formats, jackson}

import scala.concurrent.duration._
import scala.util.Random

class GraphScenario extends Simulation {
  private val config = ConfigFactory.load()

  implicit def asFiniteDuration(d: java.time.Duration) =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  implicit val serialization = jackson.Serialization
  implicit val formats: Formats = DefaultFormats

  val baseUrl = config.getString("loadtest.baseUrl")
//  val baseUrl = "http://39.102.74.2:8081"
  val rampupUsers = config.getInt("loadtest.rampup.users")
  val rampupTime = config.getDuration("loadtest.rampup.time")

  val httpConf = http.baseUrl(baseUrl).shareConnections.contentTypeHeader("application/json")

  val rand = new Random()

  val nodeIdFeeder = csv("nodeId.csv")
  val tagsFeeder = csv("tags.csv", '|').random

  val randomTags = Seq(
    "conversational messaging bot",
    "messaging",
    "mobile engagement",
    "personalized mobile campaigns",
    "Machine learning",
    "point-of-sale transaction",
    "guest frequency",
    "Digital strategy",
    "cloud communication",
    "interactive voice response"
  )

  val companySeq = Seq(
    "huawei",
    "ericsson",
    "lenovo",
    "nokia",
    "zte",
    "google",
    "ChinaTelecom",
    "Qualcom",
    "ChinaUnicom",
    "vivo",
    "ChinaMobile",
    "GSMA",
    "Oppo",
    "CICT"
  )

  val speakerSeq = Seq(
    "Ray",
    "SpeakerA",
    "SpeakerB",
    "SpeakerC",
    "SpeakerD",
    "SpeakerE"
  )

  val audienceSeq = Seq(
    "developer",
    "sales",
    "product manager",
    "executive",
    "user"
  )

  val companyFeeder = csv("companyId.csv").random
  val userLabelsFeeder = csv("userLabels.csv")

  def nodeReq(session: Session): CreateNodeReq = {
    val nodeId = session("nodeId").as[String]
    println(nodeId)
    val x = nodeId.substring(1,2)
    val y = nodeId.substring(3,4)
    println(s"($x, $y)")
    val nodetype = session("nodetype").as[String]
    val tag = session("tag").as[String]

    val tags = Map(
      session("primary").as[String] -> 1000,
      session("second1").as[String] -> 200,
      session("second2").as[String] -> 200
    )

    val company = session("company").as[String]

    CreateNodeReq(
      nodeId,
      nodetype,
      company,
      tags,
      Map("x" -> x, "y" -> y, "title" -> s"$company-$nodeId")
    )
  }

  val nodeScn = scenario("create nodes")
    .feed(nodeIdFeeder)
    .feed(tagsFeeder)
    .feed(userLabelsFeeder)
    .feed(companyFeeder)
    .exec(
      http("create node")
        .put("/api/graph")
        .body(new StringBody(session => write(nodeReq(session))))
        .check(status.is(200))
    )

  val userIdFeeder = csv("userIds.csv")

  val userScn = scenario("create users")
    .feed(userIdFeeder)
    .feed(userLabelsFeeder)
    .exec(
      http("create node")
        .put("/api/user")
        .body(new StringBody(session => write(
          CreateUserReq(
            session("userId").as[String],
            "user",
            Map(
              "x" -> session("userId").as[String].substring(1,2),
              "y" -> session("userId").as[String].substring(2,3),
              "firstname" -> session("userId").as[String].substring(1,2),
              "lastname" -> session("userId").as[String].substring(2,3)
            ),
            Map(
              session("primary").as[String] -> 5000,
              session("second1").as[String] -> 1000,
              session("second2").as[String] -> 1000
            )
          )
        )))
        .check(status.is(200))
    )

  val edgeTypeFeeder = csv("edgeType.csv").random
  val edgeNodeIdFeeder = csv("nodeId.csv").random
  val targetNodeIdFeeder = csv("targetNodeId.csv").random

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

  def buildNodeVisitReq(session: Session): NodeVisitReq = {
    val userInfoStr = session("userinfo").as[String]
    val userInfo = JsonMethods.parse(userInfoStr).extract[UserInfo]

    NodeVisitReq(
      nodeId = session("nodeId").as[String],
      targetNodeId = session("targetNodeId").as[String],
      userId = session("userId").as[String],
      userLabels = userInfo.labels
    )
  }

  def buildNodeReferralReq(session: Session): NodeReferralReq = {
    val userInfoStr = session("userinfo").as[String]
    val userInfo = JsonMethods.parse(userInfoStr).extract[UserInfo]

    val nr = NodeReferralReq(
      session("nodeId").as[String],
      session("userId").as[String],
      userInfo.labels
    )
    println(nr)
    nr
  }

  val startingNodeIdFeeder = csv("startingNodeId.csv").random
  val vistorScn = scenario("visitor scn")
    .feed(startingNodeIdFeeder.random)
    .feed(userIdFeeder.random)
    .repeat(10) {
      exec(
        http("user info")
          .get("http://localhost:8082/api/user/${userId}")
//          .get("http://39.97.181.51:8081/api/user/${userId}")
          .check(bodyString.saveAs("userinfo"))
      )
      .exec(
        http("request")
          .post("http://localhost:8083/api/request")
//          .post("http://39.97.243.209:8081/api/request")
          .body(new StringBody(session => write(buildNodeReferralReq(session))))
          .check(bodyString.saveAs("requestResponse"))
      )
      .exec{ session =>
        val requestResponseStr = session("requestResponse").as[String]
        val resp: NodeRecommendationSuccess = JsonMethods.parse(requestResponseStr).extract[NodeRecommendationSuccess]
        val selectFrom = resp.relevant// ++ resp.neighbourHistory
        val targetNode = selectFrom.drop(rand.nextInt(selectFrom.size - 1)).head
        println("x"*100)
        println(targetNode)
        println(session("userId").as[String])

        session.set("targetNodeId", targetNode.node.nodeId)
      }
      .pause(1 seconds, 5 seconds)
      .exec(
        http("record")
          .post("http://localhost:8083/api/record")
//          .post("http://39.97.243.209:8081/api/record")
          .body(new StringBody(session => write(buildNodeVisitReq(session))))
          .check(bodyString.saveAs("visitResponse"))
      )
      .exec{ session =>
        val targetNodeId = session("targetNodeId").as[String]

        session.set("nodeId", targetNodeId)
      }

    }



//  setUp(
//    nodeScn.inject(rampUsers(100) during (100 seconds))
//  ).protocols(httpConf)

  val visitorConf = http.shareConnections.contentTypeHeader("application/json")
  setUp(
    vistorScn.inject(rampUsers(1) during (1 seconds))
  ).protocols(visitorConf)


//  val userUrl = "http://39.97.181.51:8081"
//  val userUrl = "http://localhost:8082"
//  val userConf = http.baseUrl(userUrl).shareConnections.contentTypeHeader("application/json")
//  setUp(
//    userScn.inject(rampUsers(99) during (9 seconds))
//  ).protocols(userConf)
}
