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
import scala.util.Random

object SagaNodeReadSideActor {
  case class RecoWithNodeInfo(node: NodeInfo, method: String, confidence: Int)

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[RetrieveNodesQuery], name = "RetrieveNodesQuery"),
      new JsonSubTypes.Type(value = classOf[NodeInformationUpdate], name = "NodeInformationUpdate"),
      new JsonSubTypes.Type(value = classOf[RelatedNodeQuery], name = "RelatedNodeQuery")))
  sealed trait SagaNodeReadSideCommand
  case class NodeInformationUpdate(node: NodeInfo) extends SagaNodeReadSideCommand
  case class NodeDisabled(nodeId: String, companyId: String) extends SagaNodeReadSideCommand
  case class RelatedNodeQuery(tags: Tags, replyTo: ActorRef[SagaNodesQueryResponse]) extends SagaNodeReadSideCommand
  case class RetrieveNodesQuery(companyName: Option[String], tagMatching: Map[String, Int], edgeWeight: Map[String, Int], relevant: Map[String, Int], trending: Map[String, Int], neighbourHistory: Map[String, Int], trendingByTag: Map[String, Seq[(String, Int)]], replyTo: ActorRef[SagaNodesInfoResponse]) extends SagaNodeReadSideCommand
  case class RetrieveCompanyNodesQuery(companyName: String, number: Int, replyTo: ActorRef[SagaNodeReadSideResponse]) extends SagaNodeReadSideCommand

  sealed trait SagaNodeReadSideResponse
  case class SagaNodesQueryResponse(list: Map[String, Int]) extends SagaNodeReadSideResponse
  case class SagaNodesInfoResponse(tagMatching: Seq[RecoWithNodeInfo], edgeWeight: Seq[RecoWithNodeInfo], relevant: Seq[RecoWithNodeInfo], trending: Seq[RecoWithNodeInfo], neighbourHistory: Seq[RecoWithNodeInfo], trendingByTag: Map[String, Seq[RecoWithNodeInfo]], companyNodes: Option[Seq[NodeInfo]]) extends SagaNodeReadSideResponse
  case class SagaCompanyNodesResponse(companyNodes: Option[Seq[NodeInfo]]) extends SagaNodeReadSideResponse

  def apply(numberOfRecommendationsToTake: Int)(implicit session: Session): Behavior[SagaNodeReadSideCommand] =
    Behaviors.setup[SagaNodeReadSideCommand] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

      val rand = new Random()
//      println("SagaNodeReadSideActor started")

      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(NodeUpdateEventName, NoOffset)
      createdStream
        .map {
          case ee@EventEnvelope(_, _, _, value: GraphNodeUpdated) =>
            context.self ! NodeInformationUpdate(NodeInfo(value.companyId, value.id, value.nodeType, value.tags, value.properties))
            ee
          case ee@EventEnvelope(_, _, _, value: GraphNodeDisabled) =>
            context.self ! NodeDisabled(value.id, value.companyId)
            ee
          case ee =>
            ee
        }
        .runWith(Sink.ignore)

      def collectNewNode(nodeMap: HashMap[String, NodeInfo], nodesByCompany: HashMap[String, Seq[NodeInfo]]): Behavior[SagaNodeReadSideCommand] =
        Behaviors.receiveMessagePartial{
          case NodeInformationUpdate(node) =>
            if(node.nodeType == "vod") {
              collectNewNode(nodeMap, nodesByCompany)
            } else {
              println(node)

              val propertyCompanyMap = node.properties.get("company").map{ companyId =>
                val tempCList = nodesByCompany.getOrElse(companyId, Seq.empty)
                Map(companyId -> (node +: tempCList.filterNot(_.nodeId == node.nodeId)))
              }

              val clist = nodesByCompany.getOrElse(node.company, Seq.empty)
              val companyList = node +: clist.filterNot(_.nodeId == node.nodeId)

              val newCompanyMap = propertyCompanyMap.getOrElse(Map.empty) + (node.company -> companyList)
              collectNewNode(nodeMap + (node.nodeId -> node), nodesByCompany ++ newCompanyMap)
            }

          case NodeDisabled(nodeId, companyId) =>
            println(s"removing node $nodeId")
            val clist = nodesByCompany.getOrElse(companyId, Seq.empty)
            println(s"removing node ${clist.map(_.nodeId)}")
            val companyList = clist.filter(_.nodeId != nodeId)
            println(s"removed node ${companyList.map(_.nodeId)}")

            collectNewNode(nodeMap - nodeId, nodesByCompany + (companyId -> companyList))


          case RetrieveCompanyNodesQuery(company, number, replyTo) =>
            replyTo ! SagaCompanyNodesResponse(
              nodesByCompany.get(company).map{ ls =>
                if(number <= 0){
                  ls
                } else {
                  val start =
                    if(ls.size <= number) 0
                    else rand.nextInt(ls.size - number - 1)

                  ls.slice(start, start + number)
                }
              }
            )
            Behaviors.same

          case retrieval: RetrieveNodesQuery =>
            val edgeWeightNodes = retrieval.edgeWeight.flatMap{
              case (id, weight) =>
                nodeMap
                  .get(id)
                  .filterNot(_.company == retrieval.companyName.getOrElse(""))
                  .map(RecoWithNodeInfo(_, "edge-weight", weight))
            }.toSeq
            val tagMatchingNodes = retrieval.tagMatching.flatMap{
              case (id, weight) =>
                nodeMap
                  .get(id)
                  .filterNot(_.company == retrieval.companyName.getOrElse(""))
                  .map(RecoWithNodeInfo(_, "tag-matching", weight))
            }.toSeq
            val relevantNodes = retrieval.relevant.flatMap{
              case (id, weight) =>
                nodeMap
                  .get(id)
                  .filterNot(_.company == retrieval.companyName.getOrElse(""))
                  .map(RecoWithNodeInfo(_, "relevant", weight))
            }.toSeq
            val trendingNodes = retrieval.trending.flatMap{
              case (id, weight) =>
                nodeMap
                  .get(id)
                  .filterNot(_.company == retrieval.companyName.getOrElse(""))
                  .map(RecoWithNodeInfo(_, "trending", weight))
            }.toSeq
            val neighbouringNodes = retrieval.neighbourHistory.flatMap{
              case (id, weight) =>
                nodeMap
                  .get(id)
                  .filterNot(_.company == retrieval.companyName.getOrElse(""))
                  .map(RecoWithNodeInfo(_, "neighbour-history", weight))
            }.toSeq

            val trendingByTagNodes: Map[String, Seq[RecoWithNodeInfo]] = retrieval.trendingByTag.mapValues{ tagTrending =>
              tagTrending.flatMap{
                case (id, weight) =>
                  nodeMap
                    .get(id)
                    .filterNot(_.company == retrieval.companyName.getOrElse(""))
                    .map(RecoWithNodeInfo(_, "trending-by-tag", weight))
              }
            }

            val companyNodes = for {
              cn <- retrieval.companyName
              ls <- nodesByCompany.get(cn)
            } yield {
              val start =
                if(ls.size <= 5) 0
                else rand.nextInt(ls.size - 5)

              ls.slice(start, start + 5)
            }
            retrieval.replyTo ! SagaNodesInfoResponse(tagMatchingNodes, edgeWeightNodes, relevantNodes, trendingNodes, neighbouringNodes, trendingByTagNodes, companyNodes)
            Behaviors.same

          case RelatedNodeQuery(tags, replyTo) =>
            val visitorTotalWeight = tags.values.sum + 1

            val recommended = nodeMap.values.foldLeft(Seq.empty[(String, Int)]){
              case (acc, curNode) =>
                val weightTotal = curNode.tags.foldLeft(0){
                  case (weightAcc, (label, weight)) =>
                    weightAcc + (tags.getOrElse(label, 0) * (weight.toDouble / visitorTotalWeight)).toInt
                }
                (curNode.nodeId, weightTotal) +: acc
            }.sortWith(_._2 > _._2).take(numberOfRecommendationsToTake).toMap

            replyTo ! SagaNodesQueryResponse(recommended)
            Behaviors.same
        }

      collectNewNode(HashMap.empty, HashMap.empty)
    }

}
