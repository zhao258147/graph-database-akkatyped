package com.gotin.analytics

import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.typed.ClusterSingleton
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpMessage, HttpMethods, HttpRequest, MediaTypes}
import akka.http.scaladsl.server.Route
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, NoOffset, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.{Cluster, Session}
import com.example.graph.GraphNodeEntity.{GraphNodeEdgeUpdated, GraphNodeVisitorUpdated}
import com.example.graph.readside.EventTags
import com.example.user.UserNodeEntity
import com.example.user.UserNodeEntity.{BookmarkedBy, UserAutoReplyUpdate, UserBookmarked}
import com.gotin.analytics.config.AnalyticsConfig
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

object Main extends App {
  val conf = ConfigFactory.load()
  val config = conf.as[AnalyticsConfig]("AnalyticsConfig")
  implicit val formats = DefaultFormats

  import akka.actor.typed.scaladsl.adapter._

//  implicit val typedSystem: ActorSystem[AnalyticsQueryCommand] = ActorSystem(Main(), "RayDemo")
implicit val system: ActorSystem = ActorSystem("RayDemo", conf)

//  implicit val classSystem = typedSystem.toClassic
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val session: Session = Cluster.builder
    .addContactPoint(config.cassandraConfig.contactPoints)
    .withPort(config.cassandraConfig.port)
    .withCredentials(config.cassandraConfig.username, config.cassandraConfig.password)
    .build
    .connect()

  sealed trait AnalyticsQueryCommand

//  case class NodeVisit(persistenceId: String, visitorId: String, ts: Long, visitorLabels: Map[String, Int])
//  case class NodeReferral(persistenceId: String, visitorId: String, toNode: String)
//
  val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
//  val nodeStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(EventTags.CommonEvtTag, NoOffset)
//  nodeStream
//    .map {
//      case ee@EventEnvelope(_, persistenceId: String, _, value: GraphNodeEdgeUpdated) =>
//        println(persistenceId)
//        println(value)
////        val nv = NodeVisit(persistenceId, value.visitorId, value.ts, value.visitorLabels)
//        val nv = NodeReferral(persistenceId, value.visitorId, value.direction.nodeId)
//        val data =
//          s"""
//             |{
//             | "index" : "nodereferral",
//             | "data" : ${write(nv)}
//             |}
//        """.stripMargin
//        val post = HttpRequest(
//          method = HttpMethods.POST,
//          uri = config.searchURL,
//          entity = HttpEntity(ContentType(MediaTypes.`application/json`), data)
//        )
//        println(data)
//        val rsp = Http().singleRequest(post)
//        Try {
//          val s: HttpMessage = Await.result(rsp, 50000 millis)
//          println(s)
//        }
//        ee
//      case ee =>
//        ee
//    }
//    .runWith(Sink.ignore)

  val userStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(UserNodeEntity.UserEventDefaultTagName, NoOffset)
  userStream
    .map {
      case ee@EventEnvelope(_, _, _, value: BookmarkedBy) =>
        println(value)
        val data =
          s"""
             |{
             | "index" : "userbookmark",
             | "data" : ${write(value)}
             |}
        """.stripMargin
        val post = HttpRequest(
          method = HttpMethods.POST,
          uri = config.searchURL,
          entity = HttpEntity(ContentType(MediaTypes.`application/json`), data)
        )
        println(data)
        val rsp = Http().singleRequest(post)
        Try {
          val s: HttpMessage = Await.result(rsp, 50000 millis)
          println(s)
        }
        ee
      case ee =>
        ee
    }
    .runWith(Sink.ignore)
}
