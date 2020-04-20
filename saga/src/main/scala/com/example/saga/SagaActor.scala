package com.example.saga

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.datastax.driver.core.{Session, SimpleStatement}
import com.example.graph.GraphNodeEntity.{Edge, EdgeQuery, EdgeQueryResult, GraphNodeCommand, GraphNodeCommandReply, NodeId, QueryRecommended, RecommendedResult}
import com.example.user.UserNodeEntity._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object SagaActor {
  val SagaEdgeType = "User"
  sealed trait SagaActorCommand
  case class NodeReferral(nodeId: NodeId, userId: UserId, userLabels: Map[String, Int], replyTo: ActorRef[SagaActorReply]) extends SagaActorCommand
  case class RelevantNodes(nodes: Seq[NodeQueryResult]) extends SagaActorCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends SagaActorCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends SagaActorCommand

  sealed trait SagaActorReply
  case class NodeReferralReply(userId: UserId, recommended: Seq[Edge], relevant: Seq[String]) extends SagaActorReply

  case class NodeQueryResult(nodeType: String, nodeId: String, tags: java.util.Map[String, Integer], properties: java.util.Map[String, String])

  def apply(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  )(implicit session: Session): Behavior[SagaActorCommand] = Behaviors.setup { cxt =>
    println(cxt.self)
    implicit val system = cxt.system
    implicit val timeout: Timeout = 30.seconds
    implicit val ex = cxt.executionContext

    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      cxt.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    def initial(): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case referral: NodeReferral =>
          graphShardRegion ! ShardingEnvelope(
            referral.nodeId,
            QueryRecommended(
              referral.nodeId,
              referral.userId,
              Set(SagaEdgeType),
              referral.userLabels,
              Map.empty,
              nodeEntityResponseMapper
            )
          )
          waitingForNodeReply(referral)
      }

    def waitingForRelevantNodes(referral: NodeReferral, wrappedNodeReply: RecommendedResult): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case RelevantNodes(nodes) =>
          userShardRegion ! ShardingEnvelope(
            referral.userId,
            NodeVisitRequest(
              referral.userId,
              referral.nodeId,
              wrappedNodeReply.tags,
              wrappedNodeReply.edgeResult.map(_.direction.nodeId),
              nodes.map(_.nodeId),
              userEntityResponseMapper
            )
          )
          waitingForUserReply(referral, wrappedNodeReply, nodes)
      }

    def waitingForNodeReply(referral: NodeReferral): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(wrappedNodeReply: RecommendedResult) =>
            val stmt = new SimpleStatement(s"SELECT * FROM graph.nodes WHERE type='${wrappedNodeReply.nodeType}'").setFetchSize(100)
            val nodes = CassandraSource(stmt)
              .map{ row =>
                println(row)
                NodeQueryResult(
                  row.getString("type"),
                  row.getString("id"),
                  row.getMap("tags", classOf[String], classOf[java.lang.Integer]),
                  row.getMap("properties", classOf[String], classOf[String])
                )
              }
              .runWith(Sink.seq)

            cxt.pipeToSelf(nodes) {
              case Success(x) =>
                val relevantNodes = x.foldLeft(Seq.empty[NodeQueryResult]){
                  case (acc, n) if n.nodeId != wrappedNodeReply.nodeId && n.nodeType == wrappedNodeReply.nodeType && (n.tags.asScala.keySet & wrappedNodeReply.tags.keySet).size > 1  =>
                    n +: acc
                  case (acc, _) =>
                    acc
                }.sortWith((n1, n2) => (n1.tags.asScala.keySet & wrappedNodeReply.tags.keySet).size > (n2.tags.asScala.keySet & wrappedNodeReply.tags.keySet).size )

                RelevantNodes(relevantNodes)
              case Failure(e) =>
                //TODO: get most popular nodes
                RelevantNodes(Seq.empty)
            }

            waitingForRelevantNodes(referral, wrappedNodeReply)
      }

    def waitingForUserReply(referral: NodeReferral, nodeReply: RecommendedResult, relevantNodes: Seq[NodeQueryResult]): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedUserEntityResponse(wrapperUserReply: UserRequestSuccess) =>
          val recommended = wrapperUserReply.recommended.foldLeft(Seq.empty[Edge]){
            case (acc, r) =>
              nodeReply
                .edgeResult
                .find(_.direction.nodeId == r)
                .map(_ +: acc)
                .getOrElse(acc)
          }

          val relevant = wrapperUserReply.relevant.foldLeft(Seq.empty[String]){
            case (acc, r) =>
              relevantNodes
                .find(_.nodeId == r)
                .map(_.nodeId +: acc)
                .getOrElse(acc)
          }
          referral.replyTo ! NodeReferralReply(wrapperUserReply.userId, recommended, relevant)
          Behaviors.stopped
      }

    initial()
  }


}
