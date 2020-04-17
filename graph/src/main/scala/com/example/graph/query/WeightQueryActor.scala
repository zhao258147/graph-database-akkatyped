package com.example.graph.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{EdgeQuery, EdgeQueryResult, GraphNodeCommand, GraphNodeCommandReply, NodeQuery, NodeQueryResult}

object WeightQueryActor {
  sealed trait WeightQueryCommand
  case class WeightQuery(nodeIds: Set[String], replyTo: ActorRef[WeightQueryReply]) extends WeightQueryCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends WeightQueryCommand

  sealed trait WeightQueryReply
  case class Weights(collected: Set[NodeQueryResult]) extends WeightQueryReply

  val planets = Set(
    "(0H0)",
    "(0H1)",
    "(0H2)",
    "(0H3)",
    "(0H4)",
    "(0H5)",
    "(0H6)",
    "(0H7)",
    "(0H8)",
    "(0H9)",
    "(1H0)",
    "(1H1)",
    "(1H2)",
    "(1H3)",
    "(1H4)",
    "(1H5)",
    "(1H6)",
    "(1H7)",
    "(1H8)",
    "(1H9)",
    "(2H0)",
    "(2H1)",
    "(2H2)",
    "(2H3)",
    "(2H4)",
    "(2H5)",
    "(2H6)",
    "(2H7)",
    "(2H8)",
    "(2H9)",
    "(3H0)",
    "(3H1)",
    "(3H2)",
    "(3H3)",
    "(3H4)",
    "(3H5)",
    "(3H6)",
    "(3H7)",
    "(3H8)",
    "(3H9)",
    "(4H0)",
    "(4H1)",
    "(4H2)",
    "(4H3)",
    "(4H4)",
    "(4H5)",
    "(4H6)",
    "(4H7)",
    "(4H8)",
    "(4H9)",
    "(5H0)",
    "(5H1)",
    "(5H2)",
    "(5H3)",
    "(5H4)",
    "(5H5)",
    "(5H6)",
    "(5H7)",
    "(5H8)",
    "(5H9)",
    "(6H0)",
    "(6H1)",
    "(6H2)",
    "(6H3)",
    "(6H4)",
    "(6H5)",
    "(6H6)",
    "(6H7)",
    "(6H8)",
    "(6H9)",
    "(7H0)",
    "(7H1)",
    "(7H2)",
    "(7H3)",
    "(7H4)",
    "(7H5)",
    "(7H6)",
    "(7H7)",
    "(7H8)",
    "(7H9)"
  )

  def weightQueryBehaviour(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
  )(implicit session: Session): Behavior[WeightQueryCommand] = Behaviors.setup[WeightQueryCommand] { context =>
    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      context.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    def collectResp(collected: Set[NodeQueryResult], replyTo: ActorRef[WeightQueryReply]): Behavior[WeightQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(rsp: NodeQueryResult) =>
          val cur = collected + rsp
          if(cur.size == planets.size) {
            replyTo ! Weights(cur)
            Behaviors.stopped
          } else collectResp(cur, replyTo)
      }

    val initial: Behavior[WeightQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WeightQuery(nodeIds, replyTo) =>
          nodeIds.foreach{ node =>
            graphCordinator ! ShardingEnvelope(node, NodeQuery(node, nodeEntityResponseMapper))
          }
          collectResp(Set.empty, replyTo)
      }

    initial
  }
}