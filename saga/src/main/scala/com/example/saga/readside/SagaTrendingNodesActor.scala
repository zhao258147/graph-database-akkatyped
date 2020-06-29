package com.example.saga.readside

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs
import com.example.graph.GraphNodeEntity._
import com.example.graph.readside.ClickReadSideActor
import com.example.graph.readside.NodeReadSideActor._
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object SagaTrendingNodesActor {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[RetrieveClicksQuery], name = "RetrieveClicksQuery"),
      new JsonSubTypes.Type(value = classOf[OnStartNodeClickInfo], name = "OnStartNodeClickInfo"),
      new JsonSubTypes.Type(value = classOf[NodeClickInfo], name = "NodeClickInfo")))
  sealed trait SagaTrendingNodesCommand
  case class RetrieveClicksQuery(replyTo: ActorRef[TrendingNodes]) extends SagaTrendingNodesCommand
  case class NodeClickInfo(company: String, nodeId: String, tags: Set[String], ts: Long, clicks: Int) extends SagaTrendingNodesCommand
  case class OnStartNodeClickInfo(list: Seq[NodeClickInfo]) extends SagaTrendingNodesCommand

  case class TrendingNodes(overallRanking: Map[String, Int])

  def apply()(implicit session: Session): Behavior[SagaTrendingNodesCommand] =
    Behaviors.setup[SagaTrendingNodesCommand] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

      val ts = System.currentTimeMillis() - 180000000// - 180000000//- 200000
      println(ts)

      val offset = TimeBasedUUID(UUIDs.startOf(ts))
      println("offset"*10)
      println(offset)
      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(ClickReadSideActor.ClickUpdateEventName, offset)
      createdStream
        .map {
          case ee@EventEnvelope(_, _, _, value: GraphNodeClickUpdated) =>
            context.self ! NodeClickInfo(value.companyId, value.nodeId, value.tags.keySet, value.ts, value.clicks)
            NodeClickInfo(value.companyId, value.nodeId, value.tags.keySet, value.ts, value.clicks)
          case ee =>
            ee
        }
        .runWith(Sink.foreach(println))


      def calculateOverallList(list: Seq[NodeClickInfo]): Map[String, Int] = {
        val unsortedOverallList: Map[String, Int] = list.foldLeft(Map.empty[String, Int]){
          case (listAcc, nodeClick) =>
            listAcc + (nodeClick.nodeId -> (listAcc.getOrElse(nodeClick.nodeId, 0) + nodeClick.clicks))
        }
        unsortedOverallList.toSeq.sortWith(_._2 > _._2).take(20).toMap
      }

      def waitingForInitialLoad: Behavior[SagaTrendingNodesCommand] =
        Behaviors.receiveMessagePartial {
          case OnStartNodeClickInfo(list) =>
            println("x"*100)
            context.log.debug(list.toString)
            println(s"$list")

            collectNewNode(list, calculateOverallList(list))

          case RetrieveClicksQuery(replyTo) =>
            replyTo ! TrendingNodes(Map.empty)
            Behaviors.same
        }

      def collectNewNode(list: Seq[NodeClickInfo], sortedOverallList: Map[String, Int]): Behavior[SagaTrendingNodesCommand] =
        Behaviors.receiveMessagePartial{
          case click: NodeClickInfo =>
            val newList = click +: (if(list.size > 1000) list.take(900) else list)

            collectNewNode(newList, calculateOverallList(newList))

          case RetrieveClicksQuery(replyTo) =>
            replyTo ! TrendingNodes(sortedOverallList)
            Behaviors.same
        }

      collectNewNode(Seq.empty, Map.empty)
    }

}
