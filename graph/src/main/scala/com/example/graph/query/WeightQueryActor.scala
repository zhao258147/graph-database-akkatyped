package com.example.graph.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{EdgeQuery, EdgeQueryResult, GraphNodeCommand, GraphNodeCommandReply}

object WeightQueryActor {
  sealed trait WeightQueryCommand
  case class WeightQuery(nodeIds: Set[String], replyTo: ActorRef[WeightQueryReply]) extends WeightQueryCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends WeightQueryCommand

  sealed trait WeightQueryReply
  case class Weights(collected: Set[EdgeQueryResult]) extends WeightQueryReply

  val planets = Set("Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto", "Ceres", "Eris", "Makemake", "Haumea", "Sedna", "Quaoar", "Orcus", "OR10")

  def weightQueryBehaviour(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
  )(implicit session: Session): Behavior[WeightQueryCommand] = Behaviors.setup[WeightQueryCommand] { context =>
    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      context.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    def collectResp(collected: Set[EdgeQueryResult], replyTo: ActorRef[WeightQueryReply]): Behavior[WeightQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(rsp: EdgeQueryResult) =>
          val cur = collected + rsp
          println("cur" * 20)
          println(cur)
          if(cur.size == planets.size) {
            replyTo ! Weights(cur)
            Behaviors.stopped
          } else collectResp(cur, replyTo)
      }

    val initial: Behavior[WeightQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WeightQuery(nodeIds, replyTo) =>
          nodeIds.foreach{ node =>
            graphCordinator ! ShardingEnvelope(node, EdgeQuery(node, None, Map.empty, None, Map.empty, nodeEntityResponseMapper))
          }
          collectResp(Set.empty, replyTo)
      }

    initial
  }
}
