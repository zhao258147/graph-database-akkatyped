package com.example.saga.readside

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core._
import com.example.graph.GraphNodeEntity._
import com.example.graph.readside.NodeReadSideActor.{NodeInfo, _}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContextExecutor

object SagaTrendingNodesActor {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[RetrieveClicksQuery], name = "RetrieveClicksQuery"),
      new JsonSubTypes.Type(value = classOf[ClickInformationUpdate], name = "NodeInformationUpdate"),
      new JsonSubTypes.Type(value = classOf[RelatedNodeQuery], name = "RelatedNodeQuery")))
  sealed trait SagaTrendingNodesCommand
  case class ClickInformationUpdate(node: NodeInfo) extends SagaTrendingNodesCommand
  case class RetrieveClicksQuery(tagMatching: Map[String, Int], edgeWeight: Map[String, Int], relevant: Map[String, Int], trending: Map[String, Int], neighbourHistory: Map[String, Int], trendingByTag: Map[String, Seq[(String, Int)]], replyTo: ActorRef[TrendingNodes]) extends SagaTrendingNodesCommand

  case class TrendingNodes(list: Map[String, Int])

  def apply(numberOfRecommendationsToTake: Int)(implicit session: Session): Behavior[SagaTrendingNodesCommand] =
    Behaviors.setup[SagaTrendingNodesCommand] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

//      println("SagaNodeReadSideActor started")

      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(NodeUpdateEventName, NoOffset)
      createdStream
        .map {
          case ee@EventEnvelope(_, _, _, value: GraphNodeUpdated) =>
            context.self ! ClickInformationUpdate(NodeInfo(value.companyId, value.id, value.nodeType, value.tags, value.properties))
            ee
          case ee =>
            ee
        }
        .runWith(Sink.ignore)

      def collectNewNode(nodeMap: HashMap[String, NodeInfo]): Behavior[SagaTrendingNodesCommand] =
        Behaviors.receiveMessagePartial{
          case ClickInformationUpdate(node) =>
//            println(node)
            collectNewNode(nodeMap + (node.nodeId -> node))

          case clicks: RetrieveClicksQuery =>
//            val visitorTotalWeight = tags.values.sum + 1
//
//            val recommended: Map[String, Weight] = nodeMap.values.foldLeft(Seq.empty[(String, Int)]){
//              case (acc, curNode) =>
//                val weightTotal = curNode.tags.foldLeft(0){
//                  case (weightAcc, (label, weight)) =>
//                    weightAcc + (tags.getOrElse(label, 0) * (weight.toDouble / visitorTotalWeight)).toInt
//                }
//                (curNode.nodeId, weightTotal) +: acc
//            }.sortWith(_._2 > _._2).take(numberOfRecommendationsToTake).toMap

//            replyTo ! TrendingNodes(recommended)
            Behaviors.same
        }

      collectNewNode(HashMap.empty)
    }

}
