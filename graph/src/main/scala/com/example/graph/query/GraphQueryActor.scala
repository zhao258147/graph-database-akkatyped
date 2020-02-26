package com.example.graph.query

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.datastax.driver.core.{Session, SimpleStatement}
import com.example.graph.GraphNodeEntity._
import com.example.graph.http.Requests.QueryReq

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object GraphQueryActor {
  sealed trait GraphQueryCommand
  case class NodesInformation(nodes: Set[String]) extends GraphQueryCommand
  case class NodesInformationFailed(error: String) extends GraphQueryCommand
  case class GraphQuery(graphQueries: List[QueryReq], replyTo: ActorRef[GraphQueryReply]) extends GraphQueryCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends GraphQueryCommand
  case class CheckProgress(response: ActorRef[GraphQueryReply]) extends GraphQueryCommand

  sealed trait GraphQueryReply
  case class GraphQueryReplySuccess(list: Set[Seq[TargetNodeId]]) extends GraphQueryReply
  case class GraphQueryReplyFailed(error: String) extends GraphQueryReply
  case class GraphQueryReplyProgress(allCollectedPairs: Seq[Set[PairNodes]], curNodes: Set[TargetNodeId]) extends GraphQueryReply

  case class PairNodes(fromNodeId: TargetNodeId, toNodeId: TargetNodeId)
  case class CollectedNodes(list: Seq[Set[PairNodes]])

  val emptyTargetNodeId = ""

  def buildMap(list: Seq[Set[PairNodes]], nodeId: TargetNodeId, curResult: Seq[TargetNodeId]): Set[Seq[TargetNodeId]] = {
    list.headOption match {
      case None =>
        Set(curResult)
      case Some(linkedNodes) =>
        linkedNodes.foldLeft(Set.empty[Seq[String]]) {
          case (acc, linkedNodes) =>
            if(linkedNodes.toNodeId == nodeId)
              acc ++ buildMap(list.tail, linkedNodes.fromNodeId, nodeId +: curResult)
            else acc

        }
      }
  }

  def GraphQueryBehaviour(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
  )(implicit session: Session): Behavior[GraphQueryCommand] = Behaviors.setup[GraphQueryCommand] { context =>
    implicit val system = context.system
    implicit val timeout: Timeout = 30.seconds
    implicit val ex = context.executionContext
    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      context.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    def queryInfo(allCollectedPairs: Seq[Set[PairNodes]], graph: List[QueryReq], replyTo: ActorRef[GraphQueryReply], curQuery: QueryReq, curNodes: Set[TargetNodeId] = Set.empty, nextNodes: Set[PairNodes] = Set.empty): Behavior[GraphQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(EdgeQueryResult(nodeId, edges: Set[Edge], nodeResult)) =>
          Thread.sleep(1000)
          val collectedNodes = curNodes + nodeId
          val toIds = edges.map(_.direction.nodeId)
          val toCollect = toIds.map(PairNodes(nodeId, _)) ++ nextNodes

          val allCollected = allCollectedPairs.headOption.exists{ nodes =>
            nodes.map(_.toNodeId).subsetOf(collectedNodes)
          }

          val updatedAllCollectedPairs = if(!nodeResult) {
            allCollectedPairs.headOption.map{ nodes =>
              nodes.filter(_.toNodeId != nodeId) +: allCollectedPairs.tail
            }.getOrElse(allCollectedPairs)
          } else allCollectedPairs

          if(allCollected) {
            // when all nodes have successfully returned
            val newStateList = toCollect +: updatedAllCollectedPairs

            if(toCollect.isEmpty && !graph.isEmpty){
              //case 1, if search result is exhausted, reply with empty and stop
              replyTo ! GraphQueryReplySuccess(Set.empty)
              Behaviors.stopped
            } else {
              // case 2, continue with next query
              graph.headOption.map { head: QueryReq =>
                toCollect.foreach{ linkedNodes: PairNodes =>
                  graphCordinator ! ShardingEnvelope(linkedNodes.toNodeId, EdgeQuery(linkedNodes.toNodeId, head.nodeType, head.nodeProperties, head.edgeType, head.edgeProperties, nodeEntityResponseMapper))
                }
                queryInfo(newStateList, graph.tail, replyTo, head)
              }.getOrElse{
                //case 3, no more queries, compute all connected nodes and return
                val allGraphs: Set[Seq[TargetNodeId]] = toCollect.foldLeft(Set.empty[Seq[TargetNodeId]]){
                  case (acc, linkedNodes: PairNodes) =>
                    acc ++ buildMap(newStateList, linkedNodes.toNodeId, Seq.empty)
                }
                replyTo ! GraphQueryReplySuccess(allGraphs)
                Behaviors.stopped
              }
            }
          } else {
            //wait for all nodes to be returned for the current graph query
            queryInfo(updatedAllCollectedPairs, graph, replyTo, curQuery, collectedNodes, toCollect)
          }

        case CheckProgress(replyTo) =>
          replyTo ! GraphQueryReplyProgress(allCollectedPairs, curNodes)
          Behaviors.same
      }

    def nodesInfo(graphQueries: List[QueryReq], replyTo: ActorRef[GraphQueryReply]): Behavior[GraphQueryCommand] =
      Behaviors.receiveMessagePartial {
        case NodesInformation(nodes) =>
          graphQueries.headOption.map { head =>
            nodes.foreach{ nodeId =>
              graphCordinator ! ShardingEnvelope(nodeId, EdgeQuery(nodeId, head.nodeType, head.nodeProperties, head.edgeType, head.edgeProperties, nodeEntityResponseMapper))
            }
            queryInfo(Seq(nodes.map(PairNodes(emptyTargetNodeId, _))), graphQueries.tail, replyTo, head)
          }.getOrElse{
            replyTo ! GraphQueryReplySuccess(nodes.map(Seq(_)))
            Behaviors.stopped
          }

        case NodesInformationFailed(e) =>
          replyTo ! GraphQueryReplyFailed(e)
          Behaviors.stopped
      }

    val initial: Behavior[GraphQueryCommand] =
      Behaviors.receiveMessagePartial {
        case GraphQuery(graphQueries, replyTo) =>
          val nextBehaviour = for {
            query <- graphQueries.headOption
            nodeType <- query.nodeType
          } yield {
            val stmt = query.nodeType match {
              case Some(nodeType) =>
                new SimpleStatement(s"SELECT * FROM graph.nodes WHERE type = '$nodeType'")
              case None =>
                new SimpleStatement(s"SELECT * FROM graph.nodes")
            }

            val nodes = CassandraSource(stmt)
              .map(_.getString("id"))
              .runWith(Sink.seq)

            context.pipeToSelf(nodes) {
              case Success(x) =>
                NodesInformation(x.toSet)
              case Failure(e) =>
                NodesInformationFailed(e.getMessage)
            }

            nodesInfo(graphQueries, replyTo)
          }

          nextBehaviour.getOrElse{
            replyTo ! GraphQueryReplyFailed("empty query list")
            Behaviors.stopped
          }
      }

    initial
  }
}
